"""
Main DAG for UKG Pro → Maconomy Employee Sync.
Fetches recently changed employees from UKG Pro and triggers per-employee sync.
"""
import logging
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
import rail

log = logging.getLogger(__name__)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """
    Create main DAG for fetching recently changed employees.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'ukgpro_mn_employee_sync_main_{config.instance}',
        description=(
            'Fetch recently changed employees from UKG Pro and '
            'trigger Maconomy sync'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        tags=['ukgpro_maconomy', 'employee_sync', 'main'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        def prepare_sync_timestamps():
            """
            Capture current timestamp and get last sync time.
            Returns dict with both timestamps to prevent race conditions.
            """
            variable_key = (
                f'ukgpro_mn_{config.instance}_employee_sync_last_run'
            )

            current_time = (
                datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+ 'Z'
            )

            try:
                last_sync_time = Variable.get(variable_key)
                log.info(
                    "Retrieved last sync time from Variable: %s",
                    last_sync_time
                )
            except KeyError:
                last_sync_time = config.initial_sync_time
                log.info(
                    "Variable %s not found, using initial sync time: %s",
                    variable_key, last_sync_time
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
            """Build date range using the captured timestamps."""
            timestamps = rail.result('prepare_sync_timestamps')
            last_sync_time = timestamps['last_sync_time']
            current_time = timestamps['current_sync_time']
            return f'{{{last_sync_time},{current_time}}}'

        get_recently_changed_employees = rail.UKGProEmploymentOperator(
            task_id='get_recently_changed_employees',
            ukgpro_conn_id=config.ukgpro_conn_id,
            date_time_changed=build_date_time_range,
            additional_fields=[
                'employeeID', 'employeeNumber', 'companyID', 'companyName',
                'lastHireDate', 'jobDescription',
                'supervisorEmployeeNumber', 'orgLevel2Code',
                'dateOfTermination', 'employeeStatusCode',
                'workPhoneNumber',
            ],
        )

        def extract_employee_list():
            """Validate response and return list of employees to process."""
            changes = rail.result('get_recently_changed_employees')
            if not changes:
                log.info("No recently changed employees found")
                return []

            if not isinstance(changes, list):
                log.warning("Expected list response, got %s", type(changes))
                return []

            employee_list = []
            for emp in changes:
                employee_id = emp.get('employeeID')
                employee_number = emp.get('employeeNumber')
                company_id = emp.get('companyID')

                if employee_id and employee_number and company_id:
                    employee_list.append(emp)
                else:
                    log.warning("Skipping employee with missing data: %s", emp)

            log.info("Found %d employees to process", len(employee_list))
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
            log.info(
                "No recently changed employees found in UKG Pro "
                "(query range: %s to %s)",
                timestamps['last_sync_time'], timestamps['current_sync_time']
            )

        log_no_employees = rail.PythonOperator(
            task_id='log_no_employees',
            python_callable=log_no_employees_found
        )

        process_employees = rail.TriggerDagRunForEachItemOperator(
            task_id='process_employees',
            items=lambda: rail.result('extract_employee_list'),
            trigger_dag_id=(
                f'ukgpro_mn_employee_sync_router_{config.instance}'
            ),
            conf=lambda item: {
                **item,
                'connections': {
                    'ukgpro': config.ukgpro_conn_id,
                    'maconomy': config.maconomy_conn_id,
                },
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
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

        def update_last_sync_time():
            """
            Update the last sync time Variable with the captured timestamp.
            Uses the SAME timestamp that was used for the query to prevent
            race conditions.
            """
            variable_key = (
                f'ukgpro_mn_{config.instance}_employee_sync_last_run'
            )
            timestamps = rail.result('prepare_sync_timestamps')
            current_time = timestamps['current_sync_time']

            Variable.set(variable_key, current_time)
            log.info(
                "Updated last sync time Variable '%s' to: %s",
                variable_key, current_time
            )
            return current_time

        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            python_callable=update_last_sync_time
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=(
                "{{ var.value.get('middleware_api_base_url', '') }}"
            ),
            trigger_rule='all_done'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                f'ukgpro_mn_employee_sync_can_run_batch_{config.instance}',
                default_var='true',
            ).lower() == 'true',
            yes_task='batch_main',
            no_task='prepare_sync_timestamps',
        )

        batch_main = rail.BatchTaskRunOperator(
            task_id='batch_main',
            start_task='prepare_sync_timestamps',
            end_task='post_dag_run_details',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_main >> post_dag_run_details
        can_run_batch_task >> rail.Label('No') >> prepare_timestamps

        (
            prepare_timestamps >> get_recently_changed_employees >>
            extract_employees >> check_if_employees_exist
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
