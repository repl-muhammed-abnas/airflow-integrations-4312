from datetime import timedelta, datetime, timezone
import json
import rail
from procore_ce_integration.purchase_order_sync.utils.constants import JSON_INDENT_SPACES, RESOURCE_PURCHASE_ORDER


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_dag_id,
        description='Procore Purchase Orders Webhook Processing - Store Events to S3',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.webhook_dag_max_active_runs,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'aws_conn_id': config.aws_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='extract_webhook',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def extract_webhook_data(dag_run):
            conf = dag_run.conf
            if 'webhook' not in conf:
                return None

            webhook_data = conf['webhook']['data']
            payload = {
                'company_id': webhook_data.get('company_id'),
                'event_type': webhook_data.get('event_type'),
                'project_id': webhook_data.get('project_id'),
                'resource_id': webhook_data.get('resource_id'),
                'resource_name': webhook_data.get('resource_name'),
                'timestamp': webhook_data.get('timestamp')
            }

            for key, value in payload.items():
                if not value:
                    raise ValueError(f"Invalid required field: {key}")
                if key == 'resource_name' and value != RESOURCE_PURCHASE_ORDER:
                    raise ValueError(f"Unsupported resource_name: {value}")

            return payload

        extract_webhook = rail.PythonOperator(
            task_id='extract_webhook',
            python_callable=extract_webhook_data
        )

        should_process = rail.IfOperator(
            task_id='should_process',
            test=lambda: rail.result("extract_webhook") is not None,
            yes_task='download_existing_events',
            no_task='log_to_sumo'
        )

        download_existing_events = rail.S3DownloadFileOperator(
            task_id='download_existing_events',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.purchase_order_events_key
        )

        def get_existing_events():
            artifact = rail.result('download_existing_events')
            if not artifact:
                return {'events': {}, 'events_last_cleaned': None}
            data = rail.load_json_artifact(artifact)
            if not isinstance(data, dict):
                return {'events': {}, 'events_last_cleaned': None}
            return data

        def should_clean_this_run(existing_events):
            last_cleaned = existing_events.get('events_last_cleaned')
            if not last_cleaned:
                return True
            try:
                last_cleaned_dt = datetime.fromisoformat(
                    last_cleaned.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                hours_since_clean = (
                    now - last_cleaned_dt).total_seconds() / 3600
                return hours_since_clean >= config.event_clean_interval_hours
            except (ValueError, AttributeError):
                return True

        def remove_old_events(existing_events):
            cutoff_date = datetime.now(
                timezone.utc) - timedelta(days=config.event_retention_days)
            cleaned_events = {}

            for event_key, event_timestamp in existing_events.get('events', {}).items():
                try:
                    event_dt = datetime.fromisoformat(
                        event_timestamp.replace('Z', '+00:00'))
                    if event_dt > cutoff_date:
                        cleaned_events[event_key] = event_timestamp
                except (ValueError, AttributeError):
                    cleaned_events[event_key] = event_timestamp

            return {
                'events': cleaned_events,
                'events_last_cleaned': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            }

        def add_new_event(existing_events, webhook_data):
            if webhook_data['project_id'] and webhook_data['resource_id']:
                key = f"{webhook_data['project_id']}.{webhook_data['resource_id']}.{RESOURCE_PURCHASE_ORDER}"
            else:
                raise ValueError("Insufficient data to create event key")

            updated = {
                'events': {**existing_events.get('events', {}), key: webhook_data['timestamp']},
                'events_last_cleaned': existing_events.get('events_last_cleaned')
            }
            return updated, key

        def prepare_events_list():
            webhook_data = rail.result('extract_webhook')

            existing_events = get_existing_events()
            if should_clean_this_run(existing_events):
                cleaned_events = remove_old_events(existing_events)
            else:
                cleaned_events = existing_events

            updated_events, key = add_new_event(cleaned_events, webhook_data)
            return {
                'events': updated_events['events'],
                'events_last_cleaned': updated_events.get('events_last_cleaned'),
                'events_json': json.dumps(updated_events, indent=JSON_INDENT_SPACES),
                'total_keys': len(updated_events['events']),
                'updated_key': key,
                'timestamp': webhook_data['timestamp']
            }

        prepare_events_task = rail.PythonOperator(
            task_id='prepare_events_list',
            python_callable=prepare_events_list
        )

        upload_events_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_events_to_s3',
            aws_conn_id=config.aws_conn_id,
            source='{{ result("prepare_events_list").events_json }}',
            bucket_name=config.s3_bucket_name,
            key_name=config.purchase_order_events_key,
            replace=True
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> extract_webhook >> should_process

        should_process >> rail.Label('Yes') >> download_existing_events >> prepare_events_task >> upload_events_to_s3 >> log_to_sumo
        should_process >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
