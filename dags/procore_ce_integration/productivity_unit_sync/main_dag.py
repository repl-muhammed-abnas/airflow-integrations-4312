from datetime import timedelta, datetime
import rail
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from procore_ce_integration.productivity_unit_sync.utils.constants import RESOURCE_PRODUCTIVITY_LOG
from procore_ce_integration.initial_setup_sync.shared_utils import get_tenant_email


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Procore CE Productivity Unit Sync - Scheduled Batch Events By Project',
        integration_type='generic',
        company_key=config.instance,
        schedule_interval=timedelta(seconds=config.schedule_in_seconds),
        max_active_runs=config.main_dag_max_active_runs,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'aws_conn_id': config.aws_conn_id
        }
    ) as dag:

        procore_company_id_template = "{{conn." + \
            config.procore_conn_id + ".extra_dejson.company_id}}"

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                date_format=config.procore_webhook_date_format,
                initial_sync_time='1900-01-01T00:00:00.000Z',
                reset_after_threshold=False,
                use_param_date_format=True
            )
        )

        fetch_webhook_events = rail.S3DownloadFileOperator(
            task_id='fetch_webhook_events',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.productivity_unit_events_key
        )

        def parse_webhook_events():
            last_sync_time = rail.result('get_last_sync_time')['last_synctime']
            events_artifact = rail.result('fetch_webhook_events')
            events_data = rail.load_json_artifact(events_artifact)

            last_sync_dt = datetime.fromisoformat(
                last_sync_time.replace('Z', '+00:00'))

            logs_dict = {}
            max_timestamp = last_sync_time
            max_timestamp_dt = last_sync_dt

            if not isinstance(events_data, dict):
                return {'logs_to_sync': [], 'projects_to_sync': 0, 'max_processed_timestamp': max_timestamp}

            for event_key, event_timestamp in events_data.items():
                if event_key == '_metadata':
                    continue

                if not event_key.endswith(f'.{RESOURCE_PRODUCTIVITY_LOG}'):
                    continue

                try:
                    event_dt = datetime.fromisoformat(
                        event_timestamp.replace('Z', '+00:00'))
                    if event_dt <= last_sync_dt:
                        continue
                except (ValueError, AttributeError):
                    continue

                parts = event_key.split('.')
                if len(parts) < 3:
                    continue

                project_id = parts[0]
                log_id = parts[1]

                if project_id not in logs_dict:
                    logs_dict[project_id] = {
                        'project_id': project_id,
                        'log_ids': []
                    }
                logs_dict[project_id]['log_ids'].append(log_id)

                if event_dt > max_timestamp_dt:
                    max_timestamp_dt = event_dt
                    max_timestamp = event_timestamp

            logs_to_sync = list(logs_dict.values())

            return {
                'logs_to_sync': logs_to_sync,
                'projects_to_sync': len(logs_to_sync),
                'max_processed_timestamp': max_timestamp
            }

        process_webhook_events = rail.PythonOperator(
            task_id='process_webhook_events',
            python_callable=parse_webhook_events
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                value_to_set=rail.result('process_webhook_events')['max_processed_timestamp']
                    if rail.result('process_webhook_events') and rail.result('process_webhook_events')['max_processed_timestamp']
                    else rail.result('get_last_sync_time')['current_time']
            )
        )

        check_if_productivity_unit_to_sync = rail.IfOperator(
            task_id='check_if_productivity_unit_to_sync',
            test='{{ result("process_webhook_events").projects_to_sync > 0 }}',
            yes_task='trigger_productivity_unit_sync_child_dags',
            no_task='log_to_sumo'
        )

        trigger_productivity_unit_sync_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_productivity_unit_sync_child_dags',
            trigger_dag_id=config.child_dag_id,
            items='{{ result("process_webhook_events").logs_to_sync | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'item': item,
                'company_id': rail.render_template(procore_company_id_template)
            }
        )

        wait_for_child_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_completion',
            dag_runs='{{ result("trigger_productivity_unit_sync_child_dags") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test='{{ result("search_logs", "length") > 0 }}',
            yes_task='write_logs_csv',
            no_task='log_to_sumo'
        )

        write_logs_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_csv',
            source='{{ result("search_logs") }}',
            header=['Entity Type', 'Entity Code', 'Project ID', 'Error Message', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('entity_type','') }}",
                "{{ item.properties | attr_or_default('entity_code','') }}",
                "{{ item.properties | attr_or_default('project_id','') }}",
                "{{ item.properties | attr_or_default('error_message','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_logs_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_logs_download_link',
            artifact_name='{{ result("write_logs_csv") }}',
            output_file_name='Procore_CE_Productivity_Unit_Sync_Errors_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_error_notification = rail.EmailOperator(
            task_id='send_error_notification',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: Productivity Unit Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/productivity_unit_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> get_last_sync_time >> fetch_webhook_events >> process_webhook_events >> set_last_sync_time >> check_if_productivity_unit_to_sync

        check_if_productivity_unit_to_sync >> rail.Label('Yes') >> trigger_productivity_unit_sync_child_dags
        check_if_productivity_unit_to_sync >> rail.Label('No') >> log_to_sumo

        trigger_productivity_unit_sync_child_dags >> wait_for_child_completion >> search_logs >> if_logs_present

        if_logs_present >> rail.Label('Yes') >> write_logs_csv >> generate_logs_download_link >> send_error_notification >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        batch_task >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
