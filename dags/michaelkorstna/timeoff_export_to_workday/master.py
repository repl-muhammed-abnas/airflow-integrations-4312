from datetime import timedelta
import pendulum
from airflow.models import Variable
import rail
from pendulum import datetime
from michaelkorstna.timeoff_export_to_workday.utils import custom_methods

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'MichaelKors Timeoff Export to Workday - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2025, 10, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_start_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_start_time',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        # Store job time
        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=lambda: pendulum.now(config.time_zone).strftime('%Y%m%d_%H%M%S')
        )

        # Get all scripts
        get_file_format_uri = rail.RepliconServiceOperator(
            task_id='get_file_format_uri',
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, "displayText", config.file_format_name, "uri")
        )

        # Check if file format exists
        check_file_format = rail.IfOperator(
            task_id='check_file_format',
            test="{{ result('get_file_format_uri') | is_falsy }}",
            yes_task="fail_no_file_format",
            no_task="trigger_extract_new_timeoff"
        )

        fail_no_file_format = rail.FailOperator(
            task_id='fail_no_file_format',
            message='Required file format is not available'
        )

        # Trigger extract new timeoff records
        trigger_extract_new_timeoff = rail.TriggerDagRunOperator(
            task_id='trigger_extract_new_timeoff',
            retries=0,
            trigger_dag_id=config.extract_new_bookings_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "file_format_script_uri": "{{ result('get_file_format_uri') }}",
                "parent_ecid": "{{ dag_run_ecid() }}"
            }
        )

        # Trigger extract delta timeoff records
        trigger_extract_delta_timeoff = rail.TriggerDagRunOperator(
            task_id='trigger_extract_delta_timeoff',
            retries=0,
            trigger_dag_id=config.extract_delta_bookings_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "file_format_script_uri": "{{ result('get_file_format_uri') }}",
                "parent_ecid": "{{ dag_run_ecid() }}"
            }
        )

        # Collect DAG run IDs from both extract children
        get_extract_dag_run_ids = rail.PythonOperator(
            task_id='get_extract_dag_run_ids',
            python_callable=lambda: [
                rail.result('trigger_extract_new_timeoff'),
                rail.result('trigger_extract_delta_timeoff')
            ]
        )

        # Wait for both extract child DAGs to complete
        wait_for_extract_children = rail.WaitForDagRunsSensor(
            task_id='wait_for_extract_children',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("get_extract_dag_run_ids") }}'
        )

        # Gather local logs from extract children (validation errors for invalid employees, missing timeoff types)
        gather_extract_local_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_extract_local_logs',
            dag_runs='{{ result("get_extract_dag_run_ids") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        # Gather child logs from extract children (logs from process_records children)
        gather_process_records_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_process_records_logs',
            dag_runs='{{ result("get_extract_dag_run_ids") }}',
            dagrun_task_id='gather_child_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        # Send logs
        send_logs = rail.TriggerDagRunOperator(
            task_id='send_logs',
            retries=0,
            trigger_dag_id=config.send_logs_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "filename": custom_methods.generate_log_filename(config.company_key, rail.result('process_start_time')),
                "logs": (rail.result('gather_extract_local_logs') or []) + (rail.result('gather_process_records_logs') or [])
            }
        )

        wait_for_send_logs = rail.WaitForDagRunsSensor(
            task_id='wait_for_send_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("send_logs") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task Dependencies
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> process_start_time

        process_start_time >> get_file_format_uri
        get_file_format_uri >> check_file_format

        check_file_format >> rail.Label('Yes') >> fail_no_file_format
        check_file_format >> rail.Label('No') >> trigger_extract_new_timeoff

        trigger_extract_new_timeoff >> trigger_extract_delta_timeoff >> get_extract_dag_run_ids
        get_extract_dag_run_ids >> wait_for_extract_children >> gather_extract_local_logs
        gather_extract_local_logs >> gather_process_records_logs >> send_logs >> wait_for_send_logs >> finish

        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
