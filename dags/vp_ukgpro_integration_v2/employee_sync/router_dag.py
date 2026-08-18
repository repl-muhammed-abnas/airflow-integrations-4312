"""
Main DAG for VP UKG Pro Employee Sync.
Syncs individual employee data from UKG Pro to Vantagepoint.
"""
from datetime import timedelta
import rail
from vp_ukgpro_integration_v2.employee_sync.utils.python_callable_method import (
    check_required_fields_present,
    check_organization_exists_in_vp,
    check_employee_status_codes_match,
    check_rehire_status_method,
    check_termination_status_method,
    fail_missing_fields_for_update,
    fail_organization_not_found_for_update,
    fail_missing_fields_for_rehire,
    fail_organization_not_found_for_rehire,
    fail_status_conditions_not_met,
    fail_both_systems_inactive,
    fail_multiple_employees_found,
    fail_inactive_employee,
    fail_missing_fields,
    fail_organization_not_found,
    collect_triggered_dagrun_ids,
    capture_router_dag_error
)
from vp_ukgpro_integration_v2.employee_sync.utils.config_helper import (
    extract_dynamic_config_from_dag_run
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """
    Create main DAG for syncing individual employee data.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_ukgpro_employee_sync_v2_router_{config.instance}',
        description='Sync employees from UKG Pro to Vantagepoint',
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=['vantagepoint_ukgpro', 'employee_sync', 'router'],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:

        extract_dynamic_config = rail.PythonOperator(
            task_id='extract_dynamic_config',
            python_callable=lambda dag_run: extract_dynamic_config_from_dag_run(dag_run, config)
        )

        def get_company_id_from_conf(**context):
            return context['dag_run'].conf.get('companyID')

        def get_employee_id_from_conf(**context):
            return context['dag_run'].conf.get('employeeID')

        get_employee_details_from_ukgpro = rail.UKGProDemographicOperator(
            task_id='get_employee_details_from_ukgpro',
            ukgpro_conn_id="{{ dag_run.conf.connections.ukgpro }}",
            company_id=get_company_id_from_conf,
            employee_id=get_employee_id_from_conf
        )

        def get_compensation_endpoint(**context):
            employee_id = context['dag_run'].conf.get('employeeID')
            return (
                f'/personnel/v1/compensation-details?'
                f'employeeID={employee_id}'
            )

        get_compensation_details_from_ukgpro = rail.UKGProGenericOperator(
            task_id='get_compensation_details_from_ukgpro',
            ukgpro_conn_id="{{ dag_run.conf.connections.ukgpro }}",
            endpoint=get_compensation_endpoint,
            method='GET',
            required_fields=[
                'salaryOrHourlyCode', 'payPeriod',
                'weeklyPayRate', 'hourlyPayRate'
            ],
            extract_from_array=True,
            dag=dag
        )

        def get_vp_employee_filters(**context):
            employee_number = context['dag_run'].conf.get('employeeNumber')
            return (
                f'?filterHash[0][name]=ADPFileNumber&'
                f'filterHash[0][value]={employee_number}'
            )

        get_employee_from_vp = rail.VantagepointEmployeeOperator(
            task_id='get_employee_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/employee',
            request_method='GET',
            filters=get_vp_employee_filters
        )

        get_organizations_from_vp = rail.VantagepointSettingsListOperator(
            task_id='get_organizations_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/organization',
            request_method='GET'
        )

        def combine_employee_data(**context):
            demographic_data = rail.result(
                'get_employee_details_from_ukgpro'
            )
            employment_data = context['dag_run'].conf
            compensation_data = rail.result(
                'get_compensation_details_from_ukgpro'
            )

            combined_data = {
                **demographic_data,
                **employment_data,
                **compensation_data
            }
            return combined_data

        combine_data = rail.PythonOperator(
            task_id='combine_employee_data',
            python_callable=combine_employee_data
        )

        is_employee_exist_in_vp = rail.IfOperator(
            task_id='is_employee_exist_in_vp',
            test=lambda: len(rail.result('get_employee_from_vp')) > 0,
            yes_task='is_multiple_employee_exist_in_vp',
            no_task='check_employee_status_in_ukgpro'
        )

        is_multiple_employee_exist_in_vp = rail.IfOperator(
            task_id='is_multiple_employee_exist_in_vp',
            test=lambda: len(rail.result('get_employee_from_vp')) > 1,
            yes_task='log_multiple_employees_found',
            no_task='check_employee_status_for_update'
        )

        check_employee_status_for_update = rail.IfOperator(
            task_id='check_employee_status_for_update',
            test=check_employee_status_codes_match,
            yes_task='check_required_fields_for_update',
            no_task='check_employee_status_for_rehire'
        )

        # Validation tasks for UPDATE flow (Both Active)
        check_required_fields_for_update = rail.IfOperator(
            task_id='check_required_fields_for_update',
            test=check_required_fields_present,
            yes_task='check_organization_exists_for_update',
            no_task='log_missing_fields_for_update'
        )

        check_organization_exists_for_update = rail.IfOperator(
            task_id='check_organization_exists_for_update',
            test=check_organization_exists_in_vp,
            yes_task='trigger_employee_update',
            no_task='log_organization_not_found_for_update'
        )

        log_missing_fields_for_update = rail.PythonOperator(
            task_id='log_missing_fields_for_update',
            python_callable=fail_missing_fields_for_update
        )

        log_organization_not_found_for_update = rail.PythonOperator(
            task_id='log_organization_not_found_for_update',
            python_callable=fail_organization_not_found_for_update
        )

        check_employee_status_for_rehire = rail.IfOperator(
            task_id='check_employee_status_for_rehire',
            test=check_rehire_status_method,
            yes_task='check_required_fields_for_rehire',
            no_task='check_employee_status_for_termination'
        )

        # Validation tasks for REHIRE flow (UKG=A, VP=T)
        check_required_fields_for_rehire = rail.IfOperator(
            task_id='check_required_fields_for_rehire',
            test=check_required_fields_present,
            yes_task='check_organization_exists_for_rehire',
            no_task='log_missing_fields_for_rehire'
        )

        check_organization_exists_for_rehire = rail.IfOperator(
            task_id='check_organization_exists_for_rehire',
            test=check_organization_exists_in_vp,
            yes_task='trigger_employee_update',
            no_task='log_organization_not_found_for_rehire'
        )

        log_missing_fields_for_rehire = rail.PythonOperator(
            task_id='log_missing_fields_for_rehire',
            python_callable=fail_missing_fields_for_rehire
        )

        log_organization_not_found_for_rehire = rail.PythonOperator(
            task_id='log_organization_not_found_for_rehire',
            python_callable=fail_organization_not_found_for_rehire
        )

        check_employee_status_for_termination = rail.IfOperator(
            task_id='check_employee_status_for_termination',
            test=check_termination_status_method,
            yes_task='check_employee_status_in_vp',
            no_task='log_status_conditions_not_met'
        )

        log_status_conditions_not_met = rail.PythonOperator(
            task_id='log_status_conditions_not_met',
            python_callable=fail_status_conditions_not_met
        )

        log_both_systems_inactive = rail.PythonOperator(
            task_id='log_both_systems_inactive',
            python_callable=fail_both_systems_inactive
        )

        log_multiple_employees_found = rail.PythonOperator(
            task_id='log_multiple_employees_found',
            python_callable=fail_multiple_employees_found
        )

        check_employee_status_in_ukgpro = rail.IfOperator(
            task_id='check_employee_status_in_ukgpro',
            test=lambda: (
                rail.result('combine_employee_data').get(
                    'employeeStatusCode'
                ) == 'A'
            ),
            yes_task='check_required_fields',
            no_task='log_inactive_employee'
        )

        check_employee_status_in_vp = rail.IfOperator(
            task_id='check_employee_status_in_vp',
            test=lambda: (
                rail.result('get_employee_from_vp')[0].get('Status') == 'A'
                if rail.result('get_employee_from_vp')
                else False
            ),
            yes_task='trigger_employee_update',
            no_task='log_both_systems_inactive'
        )

        check_required_fields = rail.IfOperator(
            task_id='check_required_fields',
            test=check_required_fields_present,
            yes_task='check_organization_exists',
            no_task='log_missing_fields'
        )

        check_organization_exists = rail.IfOperator(
            task_id='check_organization_exists',
            test=check_organization_exists_in_vp,
            yes_task='trigger_employee_create',
            no_task='log_organization_not_found'
        )

        log_inactive_employee = rail.PythonOperator(
            task_id='log_inactive_employee',
            python_callable=fail_inactive_employee
        )

        log_missing_fields = rail.PythonOperator(
            task_id='log_missing_fields',
            python_callable=fail_missing_fields
        )

        log_organization_not_found = rail.PythonOperator(
            task_id='log_organization_not_found',
            python_callable=fail_organization_not_found
        )

        def build_employee_conf(operation_type):
            combined_data = rail.result('combine_employee_data')
            salary_or_hourly_code = combined_data.get('salaryOrHourlyCode')
            pay_period = (
                "Salary" if salary_or_hourly_code == 'S'
                else "Hourly" if salary_or_hourly_code == 'H'
                else None
            )
            weekly_pay_rate = combined_data.get('weeklyPayRate')
            hourly_pay_rate = combined_data.get('hourlyPayRate')

            # Get approval values based on operation type
            if operation_type == 'create':
                approved_for_processing = 'N'
                approved_for_accounting = 'N'
            else:
                existing_employee = rail.result('get_employee_from_vp')[0]
                approved_for_processing = existing_employee.get(
                    'ReadyForProcessing', 'N'
                )
                approved_for_accounting = existing_employee.get(
                    'ReadyForApproval', 'N'
                )
                available_for_crm = existing_employee.get(
                    'AvailableForCRM', 'N'
                )
                vp_employee_id = existing_employee.get('Employee')

            conf = rail.get_current_context()['dag_run'].conf
            result = {
                **combined_data,
                'approved_for_use_in_processing': (
                    approved_for_processing
                ),
                'approved_for_accounting_users': (
                    approved_for_accounting
                ),
                'payPeriod': pay_period,
                'payRate': (
                    weekly_pay_rate if salary_or_hourly_code == 'S'
                    else hourly_pay_rate if salary_or_hourly_code == 'H'
                    else None
                ),
                'jobCostOvertimePercent': (
                    '150' if salary_or_hourly_code == 'H'
                    else '100' if salary_or_hourly_code
                    else None
                ),
                'jobCostOvertime2Percent': (
                    '200' if salary_or_hourly_code == 'H'
                    else '100' if salary_or_hourly_code
                    else None
                ),
                'type': operation_type,
                'connections': conf.get('connections')
            }

            # Add available_for_crm only for create type
            if operation_type != 'create':
                result['available_for_crm'] = available_for_crm
                result['vp_employee_id'] = vp_employee_id

            return result

        def determine_update_type():
            """Determine update type based on employee status"""
            combined_data = rail.result('combine_employee_data')
            vp_employees = rail.result('get_employee_from_vp')

            ukgpro_status = combined_data.get('employeeStatusCode')
            vp_status = vp_employees[0].get('Status') if vp_employees else None

            # Determine type based on status combination
            if ukgpro_status == 'A' and vp_status == 'A':
                update_type = 'update'
            elif ukgpro_status == 'A' and vp_status == 'T':
                update_type = 'rehire'
            elif ukgpro_status == 'T':
                update_type = 'termination'
            else:
                update_type = 'update'  # default

            return build_employee_conf(update_type)

        trigger_employee_update = rail.TriggerDagRunOperator(
            task_id='trigger_employee_update',
            retries=0,
            trigger_dag_id=(
                f'vp_ukgpro_employee_sync_v2_update_{config.instance}'
            ),
            conf=determine_update_type,
            wait_for_completion=True,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        trigger_employee_create = rail.TriggerDagRunOperator(
            task_id='trigger_employee_create',
            retries=0,
            trigger_dag_id=(
                f'vp_ukgpro_employee_sync_v2_create_{config.instance}'
            ),
            conf=lambda: build_employee_conf('create'),
            wait_for_completion=True,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        collect_triggered_dagrun_id = rail.PythonOperator(
            task_id='collect_triggered_dagrun_id',
            trigger_rule='all_done',
            python_callable=collect_triggered_dagrun_ids
        )

        gather_employee_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_employee_dag_errors',
            dag_runs="{{ result('collect_triggered_dagrun_id') }}",
            dagrun_task_id='catch_employee_dag_error',
            flatten=True
        )

        catch_router_dag_error = rail.PythonOperator(
            task_id='catch_router_dag_error',
            trigger_rule='all_done',
            python_callable=capture_router_dag_error,
            op_args=[
                '{{ dag_run.conf.employeeNumber }}',
                '{{ get_error_message() }}'
            ]
        )

        (
            extract_dynamic_config >>
            get_employee_details_from_ukgpro >>
            get_compensation_details_from_ukgpro >>
            combine_data
        )
        (
            combine_data >>
            get_organizations_from_vp >>
            get_employee_from_vp >>
            is_employee_exist_in_vp
        )

        # Employee existence check branches
        (
            is_employee_exist_in_vp >>
            rail.Label('Yes - Employee exists') >>
            is_multiple_employee_exist_in_vp
        )
        (
            is_employee_exist_in_vp >>
            rail.Label('No - Employee not found') >>
            check_employee_status_in_ukgpro
        )

        # Multiple employee check branches
        (
            is_multiple_employee_exist_in_vp >>
            rail.Label('Yes - Multiple employees') >>
            log_multiple_employees_found
        )
        (
            is_multiple_employee_exist_in_vp >>
            rail.Label('No - Single employee') >>
            check_employee_status_for_update
        )

        # UPDATE flow (Both Active) - uses dedicated validation tasks
        (
            check_employee_status_for_update >>
            rail.Label('Both Active (A)') >>
            check_required_fields_for_update
        )
        (
            check_employee_status_for_update >>
            rail.Label('Not both Active') >>
            check_employee_status_for_rehire
        )

        (
            check_required_fields_for_update >>
            rail.Label('All fields present') >>
            check_organization_exists_for_update
        )
        (
            check_required_fields_for_update >>
            rail.Label('Missing fields') >>
            log_missing_fields_for_update
        )

        (
            check_organization_exists_for_update >>
            rail.Label('Org exists in VP') >>
            trigger_employee_update
        )
        (
            check_organization_exists_for_update >>
            rail.Label('Org not found') >>
            log_organization_not_found_for_update
        )

        # REHIRE flow (UKG=A, VP=T) - uses dedicated validation tasks
        (
            check_employee_status_for_rehire >>
            rail.Label('UKG=A, VP=T') >>
            check_required_fields_for_rehire
        )
        (
            check_employee_status_for_rehire >>
            rail.Label('Not A/T') >>
            check_employee_status_for_termination
        )

        (
            check_required_fields_for_rehire >>
            rail.Label('All fields present') >>
            check_organization_exists_for_rehire
        )
        (
            check_required_fields_for_rehire >>
            rail.Label('Missing fields') >>
            log_missing_fields_for_rehire
        )

        (
            check_organization_exists_for_rehire >>
            rail.Label('Org exists in VP') >>
            trigger_employee_update
        )
        (
            check_organization_exists_for_rehire >>
            rail.Label('Org not found') >>
            log_organization_not_found_for_rehire
        )

        # TERMINATION flow (UKG=T with date)
        (
            check_employee_status_for_termination >>
            rail.Label('UKG=T with date') >>
            check_employee_status_in_vp
        )
        (
            check_employee_status_for_termination >>
            rail.Label('No conditions met') >>
            log_status_conditions_not_met
        )

        (
            check_employee_status_in_vp >>
            rail.Label('VP Status Active') >>
            trigger_employee_update
        )
        (
            check_employee_status_in_vp >>
            rail.Label('VP Status not Active') >>
            log_both_systems_inactive
        )

        # Status check branches (for new employee creation flow)
        (
            check_employee_status_in_ukgpro >>
            rail.Label('Active (A)') >>
            check_required_fields
        )
        (
            check_employee_status_in_ukgpro >>
            rail.Label('Inactive (I)') >>
            log_inactive_employee
        )

        # Required fields check branches
        (
            check_required_fields >>
            rail.Label('All fields present') >>
            check_organization_exists
        )
        (
            check_required_fields >>
            rail.Label('Missing fields') >>
            log_missing_fields
        )

        # Organization existence check branches
        (
            check_organization_exists >>
            rail.Label('Org exists in VP') >>
            trigger_employee_create
        )
        (
            check_organization_exists >>
            rail.Label('Org not found') >>
            log_organization_not_found
        )

        # Child dag error propagation chain
        trigger_employee_create >> collect_triggered_dagrun_id
        trigger_employee_update >> collect_triggered_dagrun_id
        (
            collect_triggered_dagrun_id >>
            gather_employee_dag_errors >>
            catch_router_dag_error
        )

        # Main dag own failures also feed into catch
        # (all_done handles the rest)
        log_missing_fields_for_update >> catch_router_dag_error
        log_organization_not_found_for_update >> catch_router_dag_error
        log_missing_fields_for_rehire >> catch_router_dag_error
        log_organization_not_found_for_rehire >> catch_router_dag_error
        log_status_conditions_not_met >> catch_router_dag_error
        log_both_systems_inactive >> catch_router_dag_error
        log_multiple_employees_found >> catch_router_dag_error
        log_inactive_employee >> catch_router_dag_error
        log_missing_fields >> catch_router_dag_error
        log_organization_not_found >> catch_router_dag_error

        return dag


rail.for_each_instance(create_dag)
