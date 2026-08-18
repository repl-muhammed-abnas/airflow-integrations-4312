from datetime import timedelta, datetime, timezone
import json
import rail
from procore_ce_integration.subcontract_change_order_sync.utils.constants import (
    RESOURCE_CHANGE_ORDER_PACKAGE,
    EVENT_TYPE_CREATE,
    EVENT_TYPE_UPDATE
)


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_processing_dag_id,
        description='Procore Subcontract Change Order Webhook Processing - Store Events to S3',
        max_active_runs=config.max_active_runs,
        integration_type='generic',
        company_key=config.instance,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days),
            'aws_conn_id': config.aws_conn_id,
            'procore_conn_id': config.procore_conn_id
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
            webhook_data = dag_run.conf['webhook']['data']

            resource_name = webhook_data['resource_name']
            resource_id = webhook_data['resource_id']
            event_type = webhook_data['event_type']

            if resource_name != RESOURCE_CHANGE_ORDER_PACKAGE:
                return {
                    'skip_processing': True,
                    'reason': f'Not a {RESOURCE_CHANGE_ORDER_PACKAGE} event'
                }

            if event_type not in [EVENT_TYPE_CREATE, EVENT_TYPE_UPDATE]:
                return {
                    'skip_processing': True,
                    'reason': f'Event type {event_type} not supported'
                }

            return {
                'skip_processing': False,
                'cop_id': resource_id,
                'project_id': webhook_data['project_id'],
                'company_id': webhook_data['company_id'],
                'timestamp': webhook_data['timestamp'],
                'event_type': event_type
            }

        extract_webhook = rail.PythonOperator(
            task_id='extract_webhook',
            python_callable=extract_webhook_data
        )

        should_process = rail.IfOperator(
            task_id='should_process',
            test="{{ result('extract_webhook').skip_processing | sn | is_falsy }}",
            yes_task='get_co',
            no_task='log_to_sumo'
        )

        get_co = rail.ProcoreApiOperator(
            task_id='get_co',
            endpoint="/change_order_packages/{{ result('extract_webhook')['cop_id'] }}",
            method='GET',
            query_params={
                'project_id': "{{ result('extract_webhook')['project_id'] }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'id', rail.result('extract_webhook')['cop_id'], default={})
        )

        check_co = rail.IfOperator(
            task_id='check_co',
            test=lambda: bool(
                (rail.result('get_co').get('type', '') == "PrimeContractChangeOrder" and config.sync_prime_contract_change_order) or
                (rail.result('get_co').get('type', '') == "CommitmentContractChangeOrder" and config.sync_commitment_contract_change_order)
            ),
            yes_task='download_existing_events',
            no_task='log_to_sumo'
        )

        download_existing_events = rail.S3DownloadFileOperator(
            task_id='download_existing_events',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.change_order_events_key
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
            for project_id, project_data in events_dict.items():
                if project_id == '_metadata':
                    continue
                try:
                    last_updated = project_data.get('last_updated', '')
                    event_dt = datetime.fromisoformat(
                        last_updated.replace('Z', '+00:00'))
                    if event_dt > cutoff_date:
                        cleaned_events[project_id] = project_data
                except (ValueError, AttributeError):
                    cleaned_events[project_id] = project_data
            cleaned_events['_metadata'] = {
                'last_cleaned': datetime.now(
                    timezone.utc).isoformat().replace(
                    '+00:00', 'Z')}
            return cleaned_events

        def add_new_event(events_dict, webhook_data):
            project_id = str(webhook_data['project_id'])
            cop_id = int(webhook_data['cop_id'])
            timestamp = webhook_data['timestamp']

            updated_events = events_dict.copy()
            if project_id not in updated_events:
                updated_events[project_id] = {
                    'cop_ids': [],
                    'last_updated': timestamp
                }

            if cop_id not in updated_events[project_id]['cop_ids']:
                updated_events[project_id]['cop_ids'].append(cop_id)

            updated_events[project_id]['last_updated'] = timestamp
            return updated_events, project_id

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
            key_name=config.change_order_events_key,
            replace=True
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> extract_webhook

        extract_webhook >> should_process
        should_process >> rail.Label('Yes') >> get_co >> check_co
        should_process >> rail.Label('No') >> log_to_sumo

        check_co >> rail.Label('Yes') >> download_existing_events >> prepare_events_task
        check_co >> rail.Label('No') >> log_to_sumo

        prepare_events_task >> upload_events_to_s3 >> log_to_sumo

        batch_task >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
