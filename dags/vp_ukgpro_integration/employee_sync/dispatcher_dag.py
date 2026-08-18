"""
Scheduler DAG for VP UKG Pro Employee Sync.
Fetches recently changed employees from UKG Pro and triggers sync.
"""
from datetime import datetime, timedelta
from airflow.models import Variable
import rail


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """
    Create scheduler DAG for fetching recently changed employees.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_ukgpro_employee_sync_dispatcher_{config.instance}',
        description=(
            'Fetch recently changed employees from UKG Pro and trigger sync'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_ukgpro', 'employee_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        def prepare_sync_timestamps():
            """
            Capture current timestamp and get last sync time.
            Returns dict with both timestamps to prevent race conditions.
            """
            customer_id = (
                rail.get_current_context()['dag_run'].conf
                .get('customerId')
            )
            variable_key = (
                f'vp_ukgpro_employee_sync_last_run_'
                f'{config.instance}_{customer_id}'
            )

            current_time = (
                datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            )

            try:
                last_sync_time = Variable.get(variable_key)
                print(
                    f"Retrieved last sync time from Variable: "
                    f"{last_sync_time}"
                )
            except KeyError:
                last_sync_time = config.initial_sync_time
                print(
                    f"Variable {variable_key} not found, using initial "
                    f"sync time: {last_sync_time}"
                )

            return {
                'last_sync_time': last_sync_time,
                'current_sync_time': current_time
            }

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=prepare_sync_timestamps
        )

        def build_date_time_range():
            """Build date range using the captured timestamps"""
            timestamps = rail.result('prepare_sync_timestamps')
            last_sync_time = timestamps['last_sync_time']
            current_time = timestamps['current_sync_time']

            # UKG Pro expects format: {startDateTime,endDateTime}
            date_range = f'{{{last_sync_time},{current_time}}}'
            return date_range

        get_recently_changed_employees = rail.UKGProEmploymentOperator(
            task_id='get_recently_changed_employees',
            ukgpro_conn_id="{{ dag_run.conf.connections.ukgpro }}",
            date_time_changed=build_date_time_range,
            additional_fields=[
                'employeeID', 'companyID',
                'lastHireDate', 'jobDescription'
            ],
        )

        def extract_employee_list():
            changes = rail.result('get_recently_changed_employees')
            if not changes:
                print("No recently changed employees found")
                return []

            if not isinstance(changes, list):
                print(f"Expected list response, got {type(changes)}")
                return []

            company_code = Variable.get(
                "vp_ukgpro_employee_sync_company_code", None
            )
            org2_code = Variable.get(
                "vp_ukgpro_employee_sync_org_level2_code", None
            )
            org3_code = Variable.get(
                "vp_ukgpro_employee_sync_org_level3_code", None
            )

            org_vars_exist = all([company_code, org2_code, org3_code])

            employee_list = []
            for emp in changes:
                employee_id = emp.get('employeeID')
                company_id = emp.get('companyID')

                if employee_id and company_id:
                    if org_vars_exist:
                        emp.update({
                            "companyCode": company_code,
                            "orgLevel2Code": org2_code,
                            "orgLevel3Code": org3_code,
                        })
                    employee_list.append(emp)
                else:
                    print(f"Skipping employee with missing data: {emp}")

            print(f"Found {len(employee_list)} employees to process")
            return employee_list

        extract_employees = rail.PythonOperator(
            task_id='extract_employee_list',
            python_callable=extract_employee_list
        )

        check_if_employees_exist = rail.IfOperator(
            task_id='check_if_employees_exist',
            test=lambda: len(rail.result('extract_employee_list')) > 0,
            yes_task='process_employees',
            no_task='log_no_employees'
        )

        def log_no_employees_found():
            timestamps = rail.result('prepare_sync_timestamps')
            print(
                f"No recently changed employees found in UKG Pro "
                f"(query range: {timestamps['last_sync_time']} to "
                f"{timestamps['current_sync_time']})"
            )

        log_no_employees = rail.PythonOperator(
            task_id='log_no_employees',
            python_callable=log_no_employees_found
        )

        # Trigger router DAG for each employee
        process_employees = rail.TriggerDagRunForEachItemOperator(
            task_id='process_employees',
            items=lambda: rail.result('extract_employee_list'),
            trigger_dag_id=f'vp_ukgpro_employee_sync_router_{config.instance}',
            conf=lambda item: {
                **item,
                'connections': (
                    rail.get_current_context()['dag_run'].conf
                    .get('connections')
                )
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def update_last_sync_time():
            """
            Update the last sync time Variable with the captured timestamp.
            Uses the SAME timestamp that was used for the query to prevent
            race conditions.
            """
            customer_id = (
                rail.get_current_context()['dag_run'].conf
                .get('customerId')
            )
            variable_key = (
                f'vp_ukgpro_employee_sync_last_run_'
                f'{config.instance}_{customer_id}'
            )
            timestamps = rail.result('prepare_sync_timestamps')
            current_time = timestamps['current_sync_time']

            Variable.set(variable_key, current_time)
            print(
                f"Updated last sync time Variable '{variable_key}' to: "
                f"{current_time}"
            )
            return current_time

        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            python_callable=update_last_sync_time
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=Variable.get(
                'middleware_api_base_url', default_var=''
            ),
            trigger_rule='all_done'
        )

        (
            prepare_timestamps >> get_recently_changed_employees >>
            extract_employees >> check_if_employees_exist
        )

        wait_for_router_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_router_dag_runs',
            dag_runs="{{ result('process_employees') }}",
            allowed_states=['success', 'failed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_router_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_router_dag_errors',
            dag_runs="{{ result('process_employees') }}",
            dagrun_task_id='catch_router_dag_error',
            flatten=True
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test="{{ result('gather_router_dag_errors') | length > 0 }}",
            yes_task='fail_employee_sync',
            no_task='update_last_sync_time'
        )

        fail_employee_sync = rail.FailOperator(
            task_id='fail_employee_sync',
            message=(
                "{{ result('gather_router_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        (
            check_if_employees_exist >> rail.Label('Employees found') >>
            process_employees >> wait_for_router_dag_runs >>
            gather_router_dag_errors >> has_sync_errors
        )
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_employee_sync >> update_sync_time
        )
        has_sync_errors >> rail.Label('No') >> update_sync_time

        (
            check_if_employees_exist >> rail.Label('No employees') >>
            log_no_employees >> update_sync_time
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
