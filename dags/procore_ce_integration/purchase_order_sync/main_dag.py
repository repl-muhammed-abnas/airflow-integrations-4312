from datetime import timedelta, datetime
import rail
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from procore_ce_integration.purchase_order_sync.utils.constants import DOWNLOAD_LINK_EXPIRY_SECONDS
from procore_ce_integration.initial_setup_sync.shared_utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Procore to ComputerEase Purchase Order Sync - Main DAG',
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
            )
        )

        fetch_webhook_events = rail.S3DownloadFileOperator(
            task_id='fetch_webhook_events',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.purchase_order_events_key
        )

        def _parse_webhook_timestamp(timestamp_str, default_year=1900):
            try:
                return datetime.strptime(timestamp_str, config.procore_webhook_fmt)
            except (TypeError, ValueError):
                return datetime(default_year, 1, 1)

        def parse_webhook_events():
            artifact = rail.result('fetch_webhook_events')
            if not artifact:
                return {
                    'purchase_orders_to_sync': [],
                    'total_pos': 0,
                    'max_processed_timestamp': None
                }
            data = rail.load_json_artifact(artifact)
            last_processed_timestamp = rail.result('get_last_sync_time')['last_synctime']
            last_processed_dt = _parse_webhook_timestamp(last_processed_timestamp)

            pos_to_sync = {}
            max_timestamp = last_processed_dt

            for event_key, event_timestamp in data.get('events', {}).items():
                try:
                    event_dt = _parse_webhook_timestamp(event_timestamp)
                    if event_dt > last_processed_dt:
                        if event_dt > max_timestamp:
                            max_timestamp = event_dt

                        # key format: "{project_id}.{resource_id}.Purchase Order Contracts"
                        parts = event_key.split('.', 2)
                        if len(parts) >= 2:
                            project_id, resource_id = parts[0], parts[1]
                            pos_to_sync[event_key] = {
                                'project_id': project_id,
                                'resource_id': resource_id
                            }
                except (ValueError, AttributeError):
                    continue

            return {
                'purchase_orders_to_sync': list(pos_to_sync.values()),
                'total_pos': len(pos_to_sync),
                'max_processed_timestamp': max_timestamp.strftime(
                    config.procore_webhook_fmt)
            }

        process_webhook_events = rail.PythonOperator(
            task_id='process_webhook_events',
            python_callable=parse_webhook_events
        )

        def _resolve_sync_watermark():
            process_result = rail.result('process_webhook_events')
            if process_result and process_result.get('max_processed_timestamp'):
                return process_result['max_processed_timestamp']
            return rail.result('get_last_sync_time')['last_synctime']

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                value_to_set=_resolve_sync_watermark()
            )
        )

        check_if_pos_to_sync = rail.IfOperator(
            task_id='check_if_pos_to_sync',
            test=lambda: rail.result('process_webhook_events') and rail.result(
                'process_webhook_events')['total_pos'] > 0,
            yes_task='trigger_purchase_order_child_dags',
            no_task='log_to_sumo'
        )

        trigger_purchase_order_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_purchase_order_child_dags',
            items='{{ result("process_webhook_events").purchase_orders_to_sync | to_json }}',
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'project_id': item['project_id'],
                'resource_id': item['resource_id'],
                'company_id': rail.render_template(procore_company_id_template)
            }
        )

        wait_for_child_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_completion',
            dag_runs='{{ result("trigger_purchase_order_child_dags") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        has_errors = rail.IfOperator(
            task_id='has_errors',
            test="{{ result('search_logs', 'length') > 0 }}",
            yes_task='write_errors_to_csv',
            no_task='log_to_sumo'
        )

        write_errors_to_csv = rail.WriteCSVFileOperator(
            task_id='write_errors_to_csv',
            source='{{ result("search_logs") }}',
            header=['Purchase Order ID', 'Company ID', 'Project ID', 'Error Type', 'Error Message'],
            row=lambda item: [
                item['properties'].get('purchase_order_id', ''),
                item['properties'].get('company_id', ''),
                item['properties'].get('project_id', ''),
                item['properties'].get('error_type', ''),
                item['properties'].get('error_message', '')
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_errors_to_csv") }}',
            output_file_name='ProcoreComputerease_PurchaseOrderSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=DOWNLOAD_LINK_EXPIRY_SECONDS
        )

        send_error_notification = rail.EmailOperator(
            task_id='send_error_notification',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: Purchase Order Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/purchase_order_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> get_last_sync_time >> fetch_webhook_events >> process_webhook_events >> set_last_sync_time >> check_if_pos_to_sync

        check_if_pos_to_sync >> rail.Label(
            'Yes') >> trigger_purchase_order_child_dags >> wait_for_child_completion >> search_logs >> has_errors
        check_if_pos_to_sync >> rail.Label('No') >> log_to_sumo

        has_errors >> rail.Label(
            'Yes') >> write_errors_to_csv >> generate_download_link >> send_error_notification >> log_to_sumo
        has_errors >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
