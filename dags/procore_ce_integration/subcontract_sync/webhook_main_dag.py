from datetime import timedelta, datetime, timezone
import json
import rail
from procore_ce_integration.initial_setup_sync.shared_utils import is_self_originated_event



def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.webhook_processing_dag_id,
        description='Procore Subcontract Sync - Webhook Processing DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'aws_conn_id': config.aws_conn_id
        }
    ) as dag:
        
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='extract_webhook',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def extract_webhook_data(dag_run):
            conf = dag_run.conf
            if 'webhook' not in conf:
                raise ValueError("No webhook data found in DAG configuration")

            webhook_data = conf['webhook']['data']
            extracted_data = {
                'company_id': webhook_data.get('company_id'),
                'event_type': webhook_data.get('event_type'),
                'project_id': webhook_data.get('project_id'),
                'resource_id': webhook_data.get('resource_id'),
                'resource_name': webhook_data.get('resource_name'),
                'timestamp': webhook_data.get('timestamp')
            }

            if is_self_originated_event(webhook_data):
                extracted_data['skip_processing'] = True
                return extracted_data

            if not extracted_data['resource_name']:
                raise ValueError("Missing required field: resource_name")
            if not extracted_data['resource_id']:
                raise ValueError("Missing required field: resource_id")
            if not extracted_data['project_id']:
                raise ValueError("Missing required field: project_id")
            if not extracted_data['timestamp']:
                raise ValueError("Missing required field: timestamp")

            if (extracted_data['resource_name'] != config.resource_work_order_contract or
                    extracted_data['event_type'] not in config.syncable_event_types):
                extracted_data['skip_processing'] = True
            else:
                extracted_data['skip_processing'] = False

            return extracted_data

        extract_webhook = rail.PythonOperator(
            task_id='extract_webhook',
            python_callable=extract_webhook_data
        )

        should_process = rail.IfOperator(
            task_id='should_process',
            test=lambda: not rail.result("extract_webhook")['skip_processing'],
            yes_task='download_existing_events',
            no_task='log_to_sumo'
        )

        download_existing_events = rail.S3DownloadFileOperator(
            task_id='download_existing_events',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.subcontract_events_key
        )

        def get_existing_events():
            artifact = rail.result('download_existing_events')
            if not artifact:
                return {}
            existing_events = rail.load_json_artifact(artifact)
            return existing_events if isinstance(existing_events, dict) else {}

        def should_clean_this_run(existing_events):
            if '_metadata' not in existing_events:
                return True

            last_cleaned = existing_events.get(
                '_metadata', {}).get('last_cleaned')
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

        def remove_old_events(events_dict):
            cutoff_date = datetime.now(
                timezone.utc) - timedelta(days=config.event_retention_days)
            cleaned_events = {}

            for event_key, event_timestamp in events_dict.items():
                if event_key == '_metadata':
                    continue
                try:
                    event_dt = datetime.fromisoformat(
                        event_timestamp.replace('Z', '+00:00'))
                    if event_dt > cutoff_date:
                        cleaned_events[event_key] = event_timestamp
                except (ValueError, AttributeError):
                    cleaned_events[event_key] = event_timestamp

            cleaned_events['_metadata'] = {
                'last_cleaned': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            }

            return cleaned_events

        def add_new_event(events_dict, webhook_data):
            project_id = webhook_data['project_id']
            resource_id = webhook_data['resource_id']
            if not project_id or not resource_id:
                raise ValueError("Insufficient data to create event key")

            key = f"{project_id}.{resource_id}"
            updated_events = events_dict.copy()
            updated_events[key] = webhook_data['timestamp']
            return updated_events, key

        def prepare_events_list():
            webhook_data = rail.result('extract_webhook')

            existing_events = get_existing_events()
            if should_clean_this_run(existing_events):
                cleaned_events = remove_old_events(existing_events)
            else:
                cleaned_events = existing_events

            updated_events, key = add_new_event(cleaned_events, webhook_data)
            return {
                'events': updated_events,
                'events_json': json.dumps(updated_events, indent=2),
                'total_keys': len(updated_events),
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
            key_name=config.subcontract_events_key,
            replace=True
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> extract_webhook
        extract_webhook >> should_process
        should_process >> rail.Label('Yes') >> download_existing_events >> prepare_events_task
        should_process >> rail.Label('No') >> log_to_sumo

        prepare_events_task >> upload_events_to_s3 >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
