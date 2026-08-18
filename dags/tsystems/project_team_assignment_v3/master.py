from datetime import timedelta
import pendulum
import rail
from rail.lib.ecid import get_dagrun_ecid
from tsystems.project_team_assignment_v3.utils import python_callable
from tsystems.project_team_assignment_v3.utils.python_callable import check_both_tasks_failed

def create_main_dag(config):
    """
    Master DAG for T-Systems Project Team Assignment
    Processes resource reservation events and updates project team assignments
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"T-Systems Project Team Assignment {config.instance}",
        start_date=pendulum.datetime(2025, 1, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_runs,
        schedule_interval=config.schedule_interval
    ) as dag:

        # Step 1: View incoming payload for debugging
        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        # Step 2: Fetch create events using access token
        get_create_event_data = rail.SimpleHttpOperator(
            task_id='get_create_event_data',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint=config.create_event_endpoint,
            headers={
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            extra_options={
                'verify': False,
                'timeout': 300
            }
        )

        # Step 3: Fetch update events using access token
        get_update_event_data = rail.SimpleHttpOperator(
            task_id='get_update_event_data',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint=config.update_event_endpoint,
            headers={
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            extra_options={
                'verify': False,
                'timeout': 300
            }
        )

        is_get_event_data_success_or_failed_with_504 = rail.IfOperator(
            task_id="is_get_event_data_success_or_failed_with_504",
            trigger_rule = "all_done",
            test= lambda: check_both_tasks_failed(check_only_504=False),
            yes_task="if_any_one_tasks_succeeded",
            no_task="fail_dag"
        )

        if_any_one_tasks_succeeded = rail.IfOperator(
            task_id='if_any_one_tasks_succeeded',
            test=lambda: check_both_tasks_failed(check_only_504=True),
            yes_task='get_process_info',
            no_task='log_no_data_to_process'
        )

        log_no_data_to_process = rail.WriteLogOperator(
            task_id='log_no_data_to_process',
            message='No data to process',
            severity='Exception',
            properties={
                'status': 'Exception',
                'details': 'No data to process'
            }
        )

        def get_process_info_data():
            current_time = pendulum.now(config.time_zone)
            return {
                'process_start_time': current_time.strftime(config.DATETIMEFORMAT),
                'log_filename': f"Log_{config.company_key}_Project_Team_Assignment_{current_time.strftime('%Y%m%d_%H%M%S')}.csv",
                'invalid_log_filename': f"Invalid_Project_Team_Assignment_Records_{current_time.strftime('%Y%m%d_%H%M%S')}.csv"
            }

        get_process_info = rail.PythonOperator(
            task_id='get_process_info',
            python_callable=get_process_info_data
        )

        create_log = rail.CreateLogOperator(
            task_id = "create_log"
        )

        get_user_permission_set = rail.RepliconServiceOperator(
            task_id='get_user_permission_set',
            endpoint='/services/PermissionSetService1.svc/GetPermissionSetAvailabilityDetailsForPolicy',
            data={
                "policyUri": "urn:replicon:policy:user"
            },
            data_handler=lambda response: list(map(lambda item: item['permissionSet']['uri'], response))
        )

        get_formatted_event_data = rail.PythonOperator(
            task_id='get_formatted_event_data',
            python_callable=python_callable.get_formatted_event_data
        )

        check_invalid_records = rail.IfOperator(
            task_id='check_invalid_records',
            test=lambda: bool(rail.result('get_formatted_event_data', 'log')),
            yes_task='log_mandatory_feild_missing',
            no_task='process_each_event_data_for_daily_capacity'
        )

        log_mandatory_feild_missing = rail.WriteLogOperator(
            task_id='log_mandatory_feild_missing',
            items='{{ (result("get_formatted_event_data", "log") or {}).get("validation_errors", []) | tojson }}',
            log='{{ result("create_log") }}',
            message='{{ item.details | join(", ") if item.details is iterable else item.details }}',
            severity='Exception',
            properties={
                'assignment_id': '{{ item.assignment_id }}',
                'decidalo_project_id': '{{ item.decidalo_project_id }}',
                'individual_id': '{{ item.individual_id }}',
                'cost_object_id': '{{ item.cost_object_id }}',
                'search_period_start': '{{ item.search_period_start }}',
                'search_period_end': '{{ item.search_period_end }}',
                'hours': '',
                'status': '{{ item.status }}',
                'details': '{{ item.details | join(", ") if item.details is iterable else item.details }}',
            }
        )

        process_each_event_data_for_daily_capacity = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_event_data_for_daily_capacity',
            items="{{ result('get_formatted_event_data') }}",
            trigger_dag_id=config.process_each_event_data_dag_id,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                "user_permission_list": rail.result('get_user_permission_set')
            }
        )

        wait_for_process_each_event_data_for_daily_capacity = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_event_data_for_daily_capacity',
            dag_runs="{{ result('process_each_event_data_for_daily_capacity') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_each_event_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_each_event_logs',
            dag_runs="{{ result('process_each_event_data_for_daily_capacity') }}",
            dagrun_task_id='each_event_log',
            flatten=True
        )


        # Split gathered child logs into bounded batches to avoid out-of-memory.
        chunk_child_logs = rail.PythonOperator(
            task_id='chunk_child_logs',
            trigger_rule='none_failed_min_one_success',
            python_callable=lambda: python_callable.chunk_child_logs(config.log_generation_batch_size)
        )

        # Trigger one log-generation child run per batch, each writing its own CSV and email.
        process_log_generation = rail.TriggerDagRunForEachItemOperator(
            task_id='process_log_generation',
            trigger_rule='none_failed_min_one_success',
            items=lambda **kwargs: rail.result('chunk_child_logs'),
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda item, index, **kwargs: {
                'childlogs': item,
                'log_filename': python_callable.build_part_log_filename(
                    rail.result('get_process_info')['log_filename'],
                    index,
                    rail.result('chunk_child_logs', 'total_parts')
                ),
                'process_start_time': rail.result('get_process_info')['process_start_time'],
                # Attach run-global validation errors to the first batch only.
                'otherlogs': rail.result('create_log') if index == 0 else []
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message="{{ get_error_message() }}"
        )

        # Define workflow
        
        [get_create_event_data,get_update_event_data] >> is_get_event_data_success_or_failed_with_504

        is_get_event_data_success_or_failed_with_504 >> rail.Label("Yes") >> if_any_one_tasks_succeeded
        is_get_event_data_success_or_failed_with_504 >> rail.Label("No") >> fail_dag

        if_any_one_tasks_succeeded >> rail.Label("No") >> log_no_data_to_process >> log_to_sumo
        if_any_one_tasks_succeeded >> rail.Label("Yes") >> get_process_info >> create_log >> get_user_permission_set >> get_formatted_event_data >> check_invalid_records

        check_invalid_records >> rail.Label("Yes") >> log_mandatory_feild_missing >> process_each_event_data_for_daily_capacity
        check_invalid_records >> rail.Label("No") >> process_each_event_data_for_daily_capacity

        process_each_event_data_for_daily_capacity >> wait_for_process_each_event_data_for_daily_capacity >> \
            gather_each_event_logs >> chunk_child_logs >> process_log_generation >> log_to_sumo

        # # Handle valid records processing
        # check_valid_records >> rail.Label("Yes") >> process_each_event_data_for_daily_capacity >> wait_for_process_each_event_data_for_daily_capacity >> \
        #     gather_each_event_logs >> process_log_generation >> log_to_sumo

        # # Handle case when no valid records - create empty logs and proceed to log generation
        # check_valid_records >> rail.Label("No") >> process_log_generation

        return dag

rail.for_each_instance(create_main_dag)