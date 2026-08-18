from datetime import timedelta, datetime, timezone
import rail
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from procore_ce_integration.initial_setup_sync.shared_utils import get_tenant_email


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.subcontract_main_dag_id,
        description='Procore Subcontract Sync - Main DAG',
        integration_type='generic',
        company_key=config.instance,
        schedule_interval=timedelta(seconds=config.schedule_in_seconds),
        max_active_runs=config.max_active_runs,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'aws_conn_id': config.aws_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id
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
                date_format=config.procore_webhook_fmt,
                initial_sync_time='1900-01-01T00:00:00.000Z',
                reset_after_threshold=False,
                use_param_date_format=True
            ),
        )

        fetch_webhook_events = rail.S3DownloadFileOperator(
            task_id='fetch_webhook_events',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.subcontract_events_key
        )

        def _load_webhook_events():
            try:
                events_data = rail.load_json_artifact(
                    rail.result('fetch_webhook_events'))
                if not isinstance(events_data, dict):
                    return None
                return events_data
            except Exception:
                return None

        def _parse_webhook_timestamp(timestamp_str, default_year=1900):
            try:
                return datetime.strptime(timestamp_str, config.procore_webhook_fmt)
            except Exception:  # pylint: disable=broad-except
                return datetime(default_year, 1, 1, tzinfo=timezone.utc)

        def parse_webhook_events():
            events_data = _load_webhook_events()
            if not events_data:
                return {'subcontracts_to_sync': [], 'total_subcontracts': 0, 'max_processed_timestamp': None}

            last_processed_timestamp = rail.result(
                'get_last_sync_time')['last_synctime']
            last_processed_dt = _parse_webhook_timestamp(
                last_processed_timestamp)

            subcontracts_to_sync = {}
            max_processed_timestamp = last_processed_dt

            for event_key, event_timestamp in events_data.items():
                if event_key == '_metadata':
                    continue
                try:
                    event_dt = _parse_webhook_timestamp(event_timestamp)
                    if event_dt > last_processed_dt:
                        if event_dt > max_processed_timestamp:
                            max_processed_timestamp = event_dt

                        if '.' in event_key:
                            project_id, subcontract_id = event_key.split('.', 1)
                            subcontracts_to_sync[event_key] = {
                                'project_id': project_id,
                                'subcontract_id': subcontract_id
                            }
                except (ValueError, AttributeError):
                    continue

            return {
                'subcontracts_to_sync': list(subcontracts_to_sync.values()),
                'total_subcontracts': len(subcontracts_to_sync),
                'max_processed_timestamp': max_processed_timestamp.strftime(
                    config.procore_webhook_fmt)
            }

        process_webhook_events = rail.PythonOperator(
            task_id='process_webhook_events',
            python_callable=parse_webhook_events
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                value_to_set=rail.result('process_webhook_events')[
                    'max_processed_timestamp'] if rail.result('process_webhook_events') and rail.result('process_webhook_events')['max_processed_timestamp'] else rail.result('get_last_sync_time')['current_time']
            )
        )

        check_if_subcontracts_to_sync = rail.IfOperator(
            task_id='check_if_subcontracts_to_sync',
            test=lambda: rail.result('process_webhook_events') and rail.result(
                'process_webhook_events')['total_subcontracts'] > 0,
            yes_task='fetch_cost_types',
            no_task='log_to_sumo'
        )

        fetch_cost_types = rail.ComputereaseAPIOperator(
            task_id='fetch_cost_types',
            endpoint='/catalog/cost-type',
            request_method='GET',
            data_handler=lambda resp: {
                item['reference']: item['code']
                for item in resp.get('data', [])
                if item.get('reference') and item.get('code')
            }
        )

        trigger_subcontract_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_subcontract_child_dags',
            items='{{ result("process_webhook_events").subcontracts_to_sync | to_json }}',
            trigger_dag_id=config.subcontract_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'project_id': item['project_id'],
                'subcontract_id': item['subcontract_id'],
                'company_id': rail.render_template(procore_company_id_template),
                'cost_type_map': rail.result('fetch_cost_types')
            }
        )

        wait_for_child_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_completion',
            dag_runs='{{ result("trigger_subcontract_child_dags") }}',
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
            header=['Entity', 'Code', 'Procore Subcontract ID', 'Procore Project ID',
                    'Project Name', 'Message', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('entity_type','') }}",
                "{{ item.properties | attr_or_default('entity_code','') }}",
                "{{ item.properties | attr_or_default('procore_subcontract_id','') }}",
                "{{ item.properties | attr_or_default('procore_project_id','') }}",
                "{{ item.properties | attr_or_default('procore_project_name','') }}",
                "{{ item.properties | attr_or_default('error_message','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_logs_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_logs_download_link',
            artifact_name='{{ result("write_logs_csv") }}',
            output_file_name='Procore_to_Computerease_Subcontract_Sync_Logs_{{ current_time() }}.csv',
            expires_in_seconds=config.download_link_expiry_seconds
        )

        send_notification_with_logs = rail.EmailOperator(
            task_id='send_notification_with_logs',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: Subcontract Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/subcontract_sync_error.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> get_last_sync_time >> fetch_webhook_events >> process_webhook_events >> set_last_sync_time >> check_if_subcontracts_to_sync

        check_if_subcontracts_to_sync >> rail.Label(
            'Yes') >> fetch_cost_types >> trigger_subcontract_child_dags >> wait_for_child_completion >> search_logs
        check_if_subcontracts_to_sync >> rail.Label('No') >> log_to_sumo

        search_logs >> if_logs_present
        if_logs_present >> rail.Label(
            'Yes') >> write_logs_csv >> generate_logs_download_link >> send_notification_with_logs >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        batch_task >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
