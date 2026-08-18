"""
Employee Update DAG for VP UKG Pro Employee Sync.
Updates, rehires, and terminates employees in Vantagepoint from UKG Pro data.
"""
from datetime import timedelta
import rail
from vp_ukgpro_integration_v2.employee_sync.utils.python_callable_method import (
    format_date_to_yyyy_mm_dd,
    get_supervisor_employee,
    get_country_code,
    get_billing_category,
    check_job_title_match,
    warn_supervisor_not_found_for_update,
    capture_update_error
)
from vp_ukgpro_integration_v2.employee_sync.utils.config_helper import (
    extract_dynamic_config_from_dag_run
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """
    Create DAG for updating, rehiring, and terminating employees in VP.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_ukgpro_employee_sync_v2_update_{config.instance}',
        description='Sync employees from UKG Pro to Vantagepoint',
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=['vantagepoint_ukgpro', 'employee_sync', 'update_employee'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        extract_dynamic_config = rail.PythonOperator(
            task_id='extract_dynamic_config',
            python_callable=lambda dag_run: extract_dynamic_config_from_dag_run(dag_run, config)
        )

        check_type_termination = rail.IfOperator(
            task_id='check_type_termination',
            test=lambda dag_run: dag_run.conf.get('type') == 'termination',
            yes_task='terminate_employee_in_vp',
            no_task='get_job_titles_from_vp'
        )

        terminate_employee_in_vp = rail.VantagepointEmployeeOperator(
            task_id="terminate_employee_in_vp",
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/employee/{{ dag_run.conf.vp_employee_id }}',
            request_method='PUT',
            request_body={
                "Org": (
                    "{{ dag_run.conf.companyCode }}:"
                    "{{ dag_run.conf.orgLevel3Code }}:"
                    "{{ dag_run.conf.orgLevel2Code }}"
                ),
                "Status": (
                    "{{ dag_run.conf.employeeStatusCode | default('T') }}"
                ),
                "TerminationDate": "{{ dag_run.conf.dateOfTermination }}",
                "ReadyForProcessing": (
                    "{{ dag_run.conf.approved_for_use_in_processing | "
                    "default('N') }}"
                ),
                "ReadyForApproval": (
                    "{{ dag_run.conf.approved_for_accounting_users | "
                    "default('N') }}"
                )
            }
        )

        get_job_titles_from_vp = rail.VantagepointSettingsListOperator(
            task_id='get_job_titles_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/codeTable/CFGEmployeeTitle',
            request_method='GET'
        )

        is_job_title_exist_in_ukgpro = rail.IfOperator(
            task_id='is_job_title_exist_in_ukgpro',
            test=lambda dag_run: (
                dag_run.conf.get('jobTitle') is not None and
                dag_run.conf.get('jobTitle') != ''
            ),
            yes_task='is_job_title_found_in_vp',
            no_task='is_supervisor_exist_in_ukgpro'
        )

        is_job_title_found_in_vp = rail.IfOperator(
            task_id='is_job_title_found_in_vp',
            test=check_job_title_match,
            yes_task='is_supervisor_exist_in_ukgpro',
            no_task='create_job_title_in_vp'
        )

        create_job_title_in_vp = rail.VantagepointSettingsListOperator(
            task_id='create_job_title_in_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/codeTable/CFGEmployeeTitle',
            request_method='POST',
            request_body=lambda: {
                "Code": (
                    rail.get_current_context()['dag_run'].conf.get('jobTitle')
                ),
                "Description": (
                    rail.get_current_context()['dag_run'].conf.get(
                        'jobDescription'
                    )
                )
            }
        )

        is_supervisor_exist_in_ukgpro = rail.IfOperator(
            task_id='is_supervisor_exist_in_ukgpro',
            test=lambda dag_run: (
                dag_run.conf.get('supervisorEmployeeNumber') is not None and
                dag_run.conf.get('supervisorEmployeeNumber') != ''
            ),
            yes_task='get_supervisor_from_vp',
            no_task='get_billing_categories_from_vp'
        )

        get_supervisor_from_vp = rail.VantagepointEmployeeOperator(
            task_id='get_supervisor_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/employee',
            request_method='GET',
            filters=lambda: (
                f'?filterHash[0][name]=ADPFileNumber&filterHash[0][value]='
                f'{rail.get_current_context()["dag_run"].conf.get("supervisorEmployeeNumber")}'  # noqa: E501
            )
        )

        is_supervisor_exist_in_vp = rail.IfOperator(
            task_id='is_supervisor_exist_in_vp',
            test=lambda: len(rail.result('get_supervisor_from_vp')) > 0,
            yes_task='get_billing_categories_from_vp',
            no_task='log_supervisor_not_found'
        )

        log_supervisor_not_found = rail.PythonOperator(
            task_id='log_supervisor_not_found',
            python_callable=warn_supervisor_not_found_for_update
        )

        catch_employee_dag_error = rail.PythonOperator(
            task_id='catch_employee_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_update_error,
            op_args=[
                '{{ dag_run.conf.employeeNumber }}',
                '{{ dag_run.conf.type }}',
                '{{ get_error_message() }}'
            ]
        )

        get_billing_categories_from_vp = rail.VantagepointSettingsListOperator(
            task_id='get_billing_categories_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/codeTable/BTLaborCats',
            request_method='GET'
        )

        check_type_update = rail.IfOperator(
            task_id='check_type_update',
            test=lambda dag_run: dag_run.conf.get('type') == 'update',
            yes_task='update_employee_in_vp',
            no_task='check_type_rehire'
        )

        def build_request_body():
            conf = rail.get_current_context()['dag_run'].conf

            request_body = {
                "Employee": conf.get('vp_employee_id'),
                "FirstName": conf.get('firstName', None),
                "LastName": conf.get('lastName', None),
                "EMail": conf.get('emailAddress', None),
                "Org": (
                    f"{conf.get('companyCode', '')}:"
                    f"{conf.get('orgLevel3Code', '')}:"
                    f"{conf.get('orgLevel2Code', '')}"
                ),
                "Status": conf.get('employeeStatusCode', None),
                "MiddleName": conf.get('middleName', None),
                "WorkPhone": conf.get('workPhoneNumber', None),
                "Supervisor": get_supervisor_employee(),
                "Address1": conf.get('addressLine1', None),
                "Address2": conf.get('addressLine2', None),
                "City": conf.get('addressCity', None),
                "State": conf.get('addressState', None),
                "ZIP": conf.get('addressZipCode', None),
                "Country": get_country_code(conf.get('addressCountry')),
                "PreferredName": conf.get('preferredName', None),
                "Title": conf.get('jobTitle', None),
                "Location": conf.get('primaryWorkLocationCode', None),
                "ReadyForProcessing": conf.get(
                    'approved_for_use_in_processing', 'N'
                ),
                "ReadyForApproval": conf.get(
                    'approved_for_accounting_users', 'N'
                ),
                "BillingCategory": get_billing_category(),
                "AvailableForCRM": conf.get('available_for_crm', 'N'),
                "JobCostRate": conf.get('payRate', None),
                "JCOvtPct": conf.get('jobCostOvertimePercent', None),
                "JCSpecialOvtPct": conf.get('jobCostOvertime2Percent', None),
                "JobCostType": conf.get('payPeriod', None),
                "ADPFileNumber": conf.get('employeeNumber', None)
            }

            if conf.get('type') == 'update':
                request_body['TerminationDate'] = (
                    format_date_to_yyyy_mm_dd(conf.get('dateOfTermination'))
                )
                request_body['EmployeeCompanyName'] = conf.get(
                    'companyName', None
                )
                request_body['HireDate'] = (
                    format_date_to_yyyy_mm_dd(conf.get('originalHireDate'))
                )

            if conf.get('type') == 'rehire':
                request_body['HomeCompany'] = conf.get('companyCode', None)
                request_body['HireDate'] = (
                    format_date_to_yyyy_mm_dd(conf.get('lastHireDate'))
                )

            return {k: v for k, v in request_body.items() if v is not None}

        update_employee_in_vp = rail.VantagepointEmployeeOperator(
            task_id="update_employee_in_vp",
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/employee/{{ dag_run.conf.vp_employee_id }}',
            request_method='PUT',
            request_body=build_request_body
        )

        check_type_rehire = rail.IfOperator(
            task_id='check_type_rehire',
            test=lambda dag_run: dag_run.conf.get('type') == 'rehire',
            yes_task='rehire_employee_in_vp',
            no_task=None
        )

        rehire_employee_in_vp = rail.VantagepointEmployeeOperator(
            task_id="rehire_employee_in_vp",
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/employee/{{ dag_run.conf.vp_employee_id }}',
            request_method='PUT',
            request_body=build_request_body
        )

        (
            extract_dynamic_config >>
            check_type_termination >>
            rail.Label('Type equals termination') >>
            terminate_employee_in_vp
        )
        (
            check_type_termination >>
            rail.Label('Type not equals termination') >>
            get_job_titles_from_vp
        )

        get_job_titles_from_vp >> is_job_title_exist_in_ukgpro

        (
            is_job_title_exist_in_ukgpro >>
            rail.Label('Job title exists') >>
            is_job_title_found_in_vp
        )
        (
            is_job_title_exist_in_ukgpro >>
            rail.Label('Job title not found') >>
            is_supervisor_exist_in_ukgpro
        )

        (
            is_job_title_found_in_vp >>
            rail.Label('Job title found in VP') >>
            is_supervisor_exist_in_ukgpro
        )
        (
            is_job_title_found_in_vp >>
            rail.Label('Job title not found in VP') >>
            create_job_title_in_vp
        )
        create_job_title_in_vp >> is_supervisor_exist_in_ukgpro

        (
            is_supervisor_exist_in_ukgpro >>
            rail.Label('Supervisor exists') >>
            get_supervisor_from_vp
        )
        (
            is_supervisor_exist_in_ukgpro >>
            rail.Label('Supervisor not found') >>
            get_billing_categories_from_vp
        )

        get_supervisor_from_vp >> is_supervisor_exist_in_vp
        (
            is_supervisor_exist_in_vp >>
            rail.Label('Supervisor found in VP') >>
            get_billing_categories_from_vp
        )
        (
            is_supervisor_exist_in_vp >>
            rail.Label('Supervisor not found in VP') >>
            log_supervisor_not_found
        )

        (
            log_supervisor_not_found >>
            get_billing_categories_from_vp >>
            check_type_update
        )

        (
            check_type_update >>
            rail.Label('Type equals update') >>
            update_employee_in_vp
        )
        (
            check_type_update >>
            rail.Label('Type not equals update') >>
            check_type_rehire
        )

        (
            check_type_rehire >>
            rail.Label('Type equals rehire') >>
            rehire_employee_in_vp
        )

        terminate_employee_in_vp >> catch_employee_dag_error
        update_employee_in_vp >> catch_employee_dag_error
        rehire_employee_in_vp >> catch_employee_dag_error

        return dag


rail.for_each_instance(create_dag)
