"""
ViaPlus User Sync - Process Each Payload (Master DAG)

This master DAG orchestrates the user sync from Keka HR to Replicon.
It runs hourly and:
1. Fetches employees from Keka HR API
2. Filters for VPTI India employees (excluding G&A department)
3. Creates collection and validates records
4. Triggers child DAGs to process each user
5. Sends email notification with sync results

Structure matches CRL user_import_ireland_v1 pattern.
"""
from datetime import datetime, timedelta , timezone
from urllib.parse import quote
from airflow.models import Variable
import rail

from viaplus.user_sync.utils import request_payload, python_callable_methods
from viaplus.user_sync.tasks.get_user_prereqs import get_user_prereqs_task_group

null = None

# Template variable helpers for Jinja2
OPEN_BRACKETS = "{{"
CLOSE_BRACKETS = "}}"

RESET_TIME_THRESHOLD_MINS = 30
STANDARD_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S+00:00"


# pylint: disable=too-many-statements
def create_child_dag(config):
    """Create the master DAG."""

    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='ViaPlus - Keka HR User Sync to Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        # ================================================================
        # Keka API Step 1: Get access token from Keka OAuth2
        # ================================================================

        get_keka_access_token = rail.SimpleHttpOperator(
            task_id="get_keka_access_token",
            method='POST',
            http_conn_id=config.keka_login_conn_id,
            endpoint='connect/token',  # No leading slash - let connection handle base URL
            headers=request_payload.get_keka_token_headers(),
            data=request_payload.get_keka_token_request_body(config),
            execution_timeout=timedelta(minutes=5),
            log_response=True
        )

        extract_keka_token = rail.PythonOperator(
            task_id="extract_keka_token",
            python_callable=request_payload.extract_keka_access_token,
            execution_timeout=timedelta(minutes=2),
            show_return_value_in_logs=False
        )

        # ================================================================
        # Test Mode: Check if test data is passed via conf
        # ================================================================
        should_trigger_via_conf = rail.IfOperator(
            task_id='should_trigger_via_conf',
            test=lambda dag_run: bool(dag_run.conf.get('test_employees_data')),
            yes_task="use_conf_employees_data",
            no_task="get_last_sync_time"
        )

        # ================================================================
        # Test Mode: Use conf employees data (for testing without Keka API)
        # ================================================================
        use_conf_employees_data = rail.PythonOperator(
            task_id="use_conf_employees_data",
            python_callable=request_payload.get_employees_from_conf,
            execution_timeout=timedelta(minutes=2),
            show_return_value_in_logs=False
        )

        # ================================================================
        # Keka API Step 2: Fetch employees from Keka (with lastModified=last 1hr)
        # ================================================================
        def get_lastsync_time_variable(variable_name, date_format, initial_sync_time, reset_after_threshold, use_param_date_format=False):

            time_format = date_format if use_param_date_format else STANDARD_TIME_FORMAT

            def get_last_synctime(variable, last_synctime_string, current_time):
                last_synctime_datetime = datetime.strptime(
                    last_synctime_string, time_format)
                last_synctime_datetime = last_synctime_datetime.replace(tzinfo=timezone.utc)
                if last_synctime_datetime <= datetime.now(timezone.utc) - timedelta(minutes=RESET_TIME_THRESHOLD_MINS):
                    Variable.set(variable, current_time)
                return last_synctime_string
            current_time = datetime.now(timezone.utc).strftime(time_format)
            last_synctime_string = Variable.get(variable_name, default_var='')
            last_synctime = (get_last_synctime(variable_name, last_synctime_string, current_time
                                            ) if reset_after_threshold else last_synctime_string) if last_synctime_string else initial_sync_time
            try:
                last_synctime = (datetime.strptime(last_synctime, time_format)).strftime(date_format)
            except ValueError:
                if use_param_date_format:
                    # Try fallback to STANDARD_TIME_FORMAT when using custom format
                    try:
                        last_synctime = (datetime.strptime(last_synctime, STANDARD_TIME_FORMAT)).strftime(date_format)
                    except ValueError:
                        # Could not parse last_synctime, returning as-is
                        pass
            return {
                'last_synctime': last_synctime,
                'last_synctime_encoded': quote(last_synctime, safe=''),
                'current_time': current_time
            }

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.last_sync_time_var,
                date_format=config.time_format,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False
            )
        )
        fetch_employees_from_keka = rail.SimpleHttpOperator(
            task_id="fetch_employees_from_keka",
            method='GET',
            http_conn_id=config.keka_api_conn_id,
            endpoint=f"api/v1/hris/employees?lastModified={OPEN_BRACKETS}result('get_last_sync_time')['last_synctime_encoded']{CLOSE_BRACKETS}",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "Mozilla",
                "Authorization": f"Bearer {OPEN_BRACKETS}ti.xcom_pull(task_ids='extract_keka_token'){CLOSE_BRACKETS}"
            },
            execution_timeout=timedelta(minutes=30),
            log_response=True
        )

        # ================================================================
        # Merge: Select employees data from either conf or Keka API
        # ================================================================
        merge_keka_employees = rail.PythonOperator(
            task_id="merge_keka_employees",
            python_callable=request_payload.merge_keka_employees_data,
            trigger_rule='one_success',
            execution_timeout=timedelta(minutes=2),
            show_return_value_in_logs=False
        )

        # ================================================================
        # Keka API Step 3: Parse employees response
        # ================================================================
        parse_employees_response = rail.PythonOperator(
            task_id="parse_employees_response",
            python_callable=request_payload.parse_keka_employees_response_merged,
            execution_timeout=timedelta(minutes=5),
            show_return_value_in_logs=False
        )

        # ================================================================
        # Keka API Step 4: Filter employees by group names
        # ================================================================
        filter_employees = rail.PythonOperator(
            task_id="filter_employees",
            python_callable=request_payload.filter_employees_by_groups,
            execution_timeout=timedelta(minutes=10),
            show_return_value_in_logs=False
        )

        # ================================================================
        # Keka API Step 5: Transform employees to collection format
        # ================================================================
        transform_employees = rail.PythonOperator(
            task_id="transform_employees",
            python_callable=request_payload.transform_employees_for_collection,
            execution_timeout=timedelta(minutes=10),
            show_return_value_in_logs=False
        )

        # ================================================================
        # Step 5: Create input data collection from transformed data
        # ================================================================
        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source=lambda: rail.result('transform_employees'),
            name="input_data",
            columns={
                "emp_id": "emp_id",
                "first_name": "first_name",
                "last_name": "last_name",
                "middle_name": "middle_name",
                "display_name": "display_name",
                "email": "email",
                "login_name": "login_name",
                "emp_status": "emp_status",
                "job_title": "job_title",
                "department_name": "department_name",
                "location_name": "location_name",
                "legal_entity_name": "legal_entity_name",
                "start_date": "start_date",
                "end_date": "end_date",
                "sup_emp_id": "sup_emp_id"
            }
        )

        # ================================================================
        # Step 3: Check if there is input data
        # ================================================================
        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_log',
            no_task='send_no_changes_email'
        )

        send_no_changes_email = rail.EmailOperator(
            task_id='send_no_changes_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Replicon User Sync - No changes required - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        # ================================================================
        # Step 4: Create logs
        # ================================================================
        create_log = rail.CreateLogOperator(
            task_id='create_log',
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id='create_supervisor_log'
        )

        # ================================================================
        # Step 5: Query invalid records (missing required fields)
        # ================================================================
        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM input_data WHERE
                    NULLIF(emp_id, '') IS NULL OR
                    NULLIF(first_name, '') IS NULL OR
                    NULLIF(last_name, '') IS NULL OR
                    NULLIF(email, '') IS NULL OR
                    NULLIF(login_name, '') IS NULL OR
                    NULLIF(emp_status, '') IS NULL OR
                    NULLIF(department_name, '') IS NULL OR
                    NULLIF(location_name, '') IS NULL OR
                    NULLIF(start_date, '') IS NULL"""
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('create_log')}}",
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                'employee_id': item.get('emp_id', ''),
                'first_name': item.get('first_name', ''),
                'last_name': item.get('last_name', ''),
                'action': 'Validation',
                'status': 'Exception',
                "details": request_payload.get_mandatory_fields_exception_message(item)
            }
        )

        # ================================================================
        # Step 6: Query valid records
        # ================================================================
        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='valid_record',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id, * FROM input_data WHERE
                    NULLIF(emp_id, '') IS NOT NULL AND
                    NULLIF(first_name, '') IS NOT NULL AND
                    NULLIF(last_name, '') IS NOT NULL AND
                    NULLIF(email, '') IS NOT NULL AND
                    NULLIF(login_name, '') IS NOT NULL AND
                    NULLIF(emp_status, '') IS NOT NULL AND
                    NULLIF(department_name, '') IS NOT NULL AND
                    NULLIF(location_name, '') IS NOT NULL AND
                    NULLIF(start_date, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='dummy_get_user_prereqs',
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        # ================================================================
        # Step 8: Get user prerequisites (TaskGroup)
        # ================================================================
        dummy_get_user_prereqs, get_user_prereqs = get_user_prereqs_task_group(config)

        # ================================================================
        # Step 9: Query disable user records
        # ================================================================
        query_disable_user_records = rail.QueryCollectionOperator(
            task_id="query_disable_user_records",
            name='inactive_user_records',
            query=f"""SELECT * FROM valid_record WHERE NULLIF(end_date, '') IS NOT NULL"""
        )

        dummy_process_disable_users = rail.EmptyOperator(
            task_id='dummy_process_disable_users'
        )

        process_disable_users = rail.trigger_parallel_dagrun(
            task_id='process_disable_users',
            items="{{ result('query_disable_user_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_users_dagid,
            conf=lambda item: request_payload.get_process_users_conf(item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ================================================================
        # Step 10: Query active user records
        # ================================================================
        query_active_user_records = rail.QueryCollectionOperator(
            task_id="query_active_user_records",
            name='active_user_records',
            query=f"""SELECT * FROM valid_record WHERE NULLIF(end_date, '') IS NULL"""
        )

        dummy_process_active_users = rail.EmptyOperator(
            task_id='dummy_process_active_users'
        )

        process_active_users = rail.trigger_parallel_dagrun(
            task_id='process_active_users',
            items="{{ result('query_active_user_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_users_dagid,
            conf=lambda item: request_payload.get_process_users_conf(item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ================================================================
        # Step 11: Gather results from all process_users DAGs
        # ================================================================
        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: python_callable_methods.get_process_users_dag_ids(
                config.trigger_parallel_dagrun_count_process_users),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_users_dag_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        # ================================================================
        # Step 12: Process supervisor checks (queued during user processing)
        # ================================================================
        get_supervisorcheck_queued_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_queued_logs',
            log="{{ result('create_supervisor_log') }}",
            severity='Pending',
            remove_filtered_entries=True
        )

        is_supervisorcheck_queued_logs = rail.IfOperator(
            task_id='is_supervisorcheck_queued_logs',
            test="{{ result('get_supervisorcheck_queued_logs', 'length') > 0 }}",
            yes_task='process_supervisor_child_dag',
            no_task='process_log_generation'
        )

        process_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child_dag',
            retries=0,
            items="{{ result('get_supervisorcheck_queued_logs') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_supervisor_dagid,
            conf=lambda item: {
                **dict(item['properties'].items()),
                'supervisor_log': rail.result('create_supervisor_log'),
                'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_permission_sets'),
                    'displayText', config.SUPERVISOR_PERMISSION, 'uri'),
                'report_user_permission_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_permission_sets'),
                    'displayText', config.REPORT_USER_PERMISSION, 'uri'),
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # ================================================================
        # Step 13: Process log generation (CSV, email)
        # ================================================================
        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda dag_run: {
                'total_records': rail.result('create_input_data_collection', key='length'),
                'userlogs': rail.result('gather_user_logs'),
                'otherlogs': rail.result('create_log'),
                'log_filename': f"log_user_sync_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
            }
        )

        def set_lastsync_time_variable(variable_name, value_to_set):
            Variable.set(variable_name, value_to_set)

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda value_to_set: set_lastsync_time_variable(
                variable_name=config.last_sync_time_var,
                value_to_set=value_to_set
            ),
            op_kwargs={
                'value_to_set': f"{OPEN_BRACKETS}result('get_last_sync_time')['current_time']{CLOSE_BRACKETS}"
            }
        )

        # ================================================================
        # Step 14: Log to Sumo
        # ================================================================
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "no_of_user_records_fetched": "{{result('create_input_data_collection','length')}}",
                "no_of_valid_user_records": "{{result('query_valid_records','length')}}",
                "no_of_invalid_user_records": "{{result('query_invalid_records','length')}}",
                "location": "India"
            }
        )

        # ================================================================
        # Step 15: Error handling
        # ================================================================
        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_dag'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message='{{ get_error_message() }}'
        )

        # ================================================================
        # Task Dependencies (matching CRL pattern)
        # ================================================================
        # Keka API steps: get_token -> extract_token -> check conf -> (fetch_employees OR use_conf) -> merge -> parse -> filter -> transform -> collection
        get_keka_access_token >> extract_keka_token >> should_trigger_via_conf

        # Path 1: Use test data from conf
        should_trigger_via_conf >> rail.Label('Yes') >> use_conf_employees_data >> merge_keka_employees

        # Path 2: Fetch from Keka API
        should_trigger_via_conf >> rail.Label('No') >> get_last_sync_time >> fetch_employees_from_keka >> merge_keka_employees

        # Merge both paths and continue processing
        merge_keka_employees >> parse_employees_response >> filter_employees >> transform_employees
        transform_employees >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label('No') >> send_no_changes_email

        has_input_data >> rail.Label('Yes') >> create_log >> create_supervisor_log
        create_supervisor_log >> query_invalid_records >> log_invalid_records >> query_valid_records

        query_valid_records >> has_valid_records
        has_valid_records >> rail.Label('Yes') >> dummy_get_user_prereqs

        get_user_prereqs >> query_disable_user_records >> dummy_process_disable_users
        dummy_process_disable_users >> process_disable_users >> query_active_user_records

        query_active_user_records >> dummy_process_active_users >> process_active_users
        process_active_users >> get_process_users_dag_ids >> gather_user_logs

        gather_user_logs >> get_supervisorcheck_queued_logs
        get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs

        is_supervisorcheck_queued_logs >> rail.Label('No') >> process_log_generation
        has_valid_records >> rail.Label('No') >> no_valid_records_present >> process_log_generation

        is_supervisorcheck_queued_logs >> rail.Label('Yes') >> process_supervisor_child_dag
        process_supervisor_child_dag >> wait_for_supervisor_child_dag >> process_log_generation

        process_log_generation >> set_last_sync_time >> log_to_sumo >> should_fail_dag >> rail.Label('No') >> fail_dag

    return dag


rail.for_each_instance(create_child_dag)
