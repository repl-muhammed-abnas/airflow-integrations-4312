import rail
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from procore_ce_integration.change_orders_sync.utils.util import (
    is_resource_ready,
    get_change_event_duration,
    is_sync_custom_field_present
)
from procore_ce_integration.change_orders_sync.utils.constants import (
    CREATE,
    APPROVED,
    CE_STATUS_OPEN,
    PENDING_APPROVAL,
    RESOURCE_CHANGE_EVENT,
    RESOURCE_BUDGET_CHANGES,
    RESOURCE_CHANGE_ORDER_PACKAGES
)
from procore_ce_integration.initial_setup_sync.shared_utils import is_self_originated_event


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_dag_id,
        description='Procore Change Order Webhook Processing - Store Events to S3',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs_webhook_dag,
        webhook_conf=rail.WebhookConf(bearer_token_var=config.bearer_token_var),
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'aws_conn_id': config.aws_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='should_process_webhook',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        should_process_webhook = rail.IfOperator(
            task_id='should_process_webhook',
            test=lambda dag_run: not is_self_originated_event(dag_run.conf['webhook']['data']),
            yes_task='extract_webhook',
            no_task='log_to_sumo'
        )

        def extract_webhook_data(dag_run):
            webhook_data = dag_run.conf['webhook']['data']
            payload = {
                'resource_name': webhook_data.get('resource_name'),
                'company_id': webhook_data.get('company_id'),
                'event_type': webhook_data.get('event_type'),
                'project_id': webhook_data.get('project_id'),
                'event_id': webhook_data.get('resource_id'),
                'timestamp': webhook_data.get('timestamp')
            }

            for key, value in payload.items():
                if not value:
                    raise ValueError(f"Invalid required field: {key}")
                if key == 'resource_name' and value not in [
                    RESOURCE_CHANGE_EVENT,
                    RESOURCE_BUDGET_CHANGES,
                    RESOURCE_CHANGE_ORDER_PACKAGES
                ]:
                    raise ValueError(f"Unsupported resource_name: {value}")

            return payload

        extract_webhook = rail.PythonOperator(
            task_id='extract_webhook',
            python_callable=extract_webhook_data
        )

        download_existing_events = rail.S3DownloadFileOperator(
            task_id='download_existing_events',
            bucket_name=config.s3_bucket_name,
            key_name=config.budget_revision_events_key
        )

        def get_existing_events():
            artifact = rail.result('download_existing_events')
            if not artifact:
                return {'events': {}, 'events_last_cleaned': None}
            state_file = rail.load_json_artifact(artifact)
            if not isinstance(state_file, dict):
                return {'events': {}, 'events_last_cleaned': None}
            events = state_file.get('events', {})
            return {
                'events': events if isinstance(events, dict) else {},
                'events_last_cleaned': state_file.get('events_last_cleaned')
            }

        def should_clean_this_run(events_last_cleaned):
            if not events_last_cleaned:
                return True
            try:
                last_cleaned_dt = datetime.fromisoformat(events_last_cleaned.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                hours_since_clean = (now - last_cleaned_dt).total_seconds() / 3600
                return hours_since_clean >= config.event_clean_interval_hours
            except (ValueError, AttributeError):
                return True

        def remove_old_events(events_dict):
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=config.event_retention_days)
            deleted_events = {}
            remaining_events = {}
            for event_id, event_data in events_dict.items():
                try:
                    last_updated = event_data.get('last_updated', '')
                    event_dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                    if event_dt < cutoff_date and event_data.get('status') == APPROVED:
                        deleted_events[event_id] = event_data
                    else:
                        remaining_events[event_id] = event_data
                except (ValueError, AttributeError):
                    remaining_events[event_id] = event_data
            return {
                'deleted_events': deleted_events,
                'remaining_events': remaining_events
            }

        def get_events_to_process():
            existing_state = get_existing_events()
            existing_events = existing_state['events']
            events_last_cleaned = existing_state['events_last_cleaned']
            if should_clean_this_run(events_last_cleaned):
                filtered_events = remove_old_events(existing_events)
                remaining_events = filtered_events['remaining_events']
                deleted_events = filtered_events['deleted_events']
                events_last_cleaned = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            else:
                deleted_events = {}
                remaining_events = existing_events
            return {
                'deleted_events': deleted_events,
                'existing_events': remaining_events,
                'events_last_cleaned': events_last_cleaned
            }

        load_existing_events = rail.PythonOperator(
            task_id='load_existing_events',
            python_callable=get_events_to_process
        )

        should_fetch_custom_field = rail.IfOperator(
            task_id='should_fetch_custom_field',
            test=lambda: rail.result('extract_webhook')['event_type'] == CREATE,
            yes_task='fetch_custom_field',
            no_task='is_change_event'
        )

        fetch_custom_field = rail.ProcoreApiOperator(
            task_id='fetch_custom_field',
            endpoint="/companies/{{ result('extract_webhook').company_id }}/custom_field_definitions",
            method='GET',
            version='1.1',
            query_params={
                'filters[with_label]': config.SYNC_CUSTOM_FIELD_LABEL
            },
            data_handler=lambda res: f"custom_field_{res[0]['id']}" if res and len(res) > 0 else None
        )

        is_change_event = rail.IfOperator(
            task_id='is_change_event',
            test="{{ result('extract_webhook').resource_name == '" + RESOURCE_CHANGE_EVENT + "' }}",
            yes_task='fetch_change_event',
            no_task='is_change_order_package'
        )

        # Fetch change event details to check approval status
        fetch_change_event = rail.ProcoreApiOperator(
            task_id='fetch_change_event',
            endpoint="/change_events/{{ result('extract_webhook').event_id }}",
            method='GET',
            version='1.1',
            query_params={
                'project_id': "{{ result('extract_webhook').project_id }}"
            },
            data_handler=lambda res: res[0] if res and len(res) > 0 else {}
        )

        is_change_order_package = rail.IfOperator(
            task_id='is_change_order_package',
            test="{{ result('extract_webhook').resource_name == '" + RESOURCE_CHANGE_ORDER_PACKAGES + "' }}",
            yes_task='fetch_change_order_package',
            no_task='fetch_budget_change'
        )

        fetch_change_order_package = rail.ProcoreApiOperator(
            task_id='fetch_change_order_package',
            endpoint="/change_order_packages/{{ result('extract_webhook').event_id }}",
            method='GET',
            query_params={
                'project_id': "{{ result('extract_webhook').project_id }}"
            },
            data_handler=lambda res: res[0] if res and len(res) > 0 else {}
        )

        should_fetch_linked_change_event = rail.IfOperator(
            task_id='should_fetch_linked_change_event',
            test=lambda: rail.result('extract_webhook')['event_type'] == CREATE,
            yes_task='fetch_pco_linked_change_event',
            no_task='build_event_payload'
        )

        def get_change_event(response):
            cop_id = rail.result('extract_webhook')['event_id']
            for change_event in response:
                line_items = change_event['change_items']
                if line_items:
                    cop = line_items[0].get('revenue_impact', {}).get('change_order_package')
                    if cop and cop['id'] == cop_id:
                        return change_event
            return None

        fetch_pco_linked_change_event = rail.ProcoreApiOperator(
            task_id='fetch_pco_linked_change_event',
            endpoint="/change_events",
            method='GET',
            version='1.1',
            query_params=lambda: {
                'filters[updated_at]': get_change_event_duration(rail.result('extract_webhook')['timestamp']),
                'project_id': rail.result('extract_webhook')['project_id']
            },
            data={
                "filters": {
                    "prime_pco": {
                        "id": [ "with" ],
                        "operator": "includes"
                    }
                }
            },
            data_handler=get_change_event
        )


        fetch_budget_change = rail.ProcoreApiOperator(
            task_id='fetch_budget_change',
            endpoint="/projects/{{ result('extract_webhook').project_id }}/budget_changes/{{ result('extract_webhook').event_id }}",
            method='GET',
            query_params={
                'project_id': "{{ result('extract_webhook').project_id }}"
            },
            data_handler=lambda res: res[0].get('data') if res and len(res) > 0 else {}
        )

        # Check if all line items have both CO package and budget change linked
        def get_event_payload():
            webhook_data = rail.result('extract_webhook')
            state = rail.result('load_existing_events') or {}
            updated_events = state.get('existing_events', {})
            events_last_cleaned = state.get('events_last_cleaned')

            if webhook_data['event_type'] == CREATE:
                custom_field_key = rail.result('fetch_custom_field')
            else:
                custom_field_key = next(
                    (ev.get('custom_field_key') for ev in updated_events.values() if ev.get('custom_field_key')),
                    None
                )

            def initialize_change_event_payload(change_event_id, project_id):
                event = updated_events.get(str(change_event_id))
                if event is None:
                    event = {
                        'origin_id': None,
                        'project_id': project_id,
                        'event_id': change_event_id,
                        'status': PENDING_APPROVAL,
                        'custom_field_key': custom_field_key,
                        'last_updated': webhook_data['timestamp'],
                        'line_items': []
                    }
                    updated_events[str(change_event_id)] = event
                return event

            def insert_resource_line_items(event, resource_name, resource_data, targets):
                # targets: {change_item_id: {'flat_code'}}
                if event.get('origin_id'):
                    return
                is_cf_present = is_sync_custom_field_present(resource_data, custom_field_key)
                by_id = {str(li['change_item_id']): li for li in event['line_items']}
                for change_item_id, info in targets.items():
                    line_item = by_id.get(str(change_item_id))
                    if line_item is None:
                        line_item = {
                            'flat_code': info.get('flat_code'),
                            'change_item_id': change_item_id,
                            RESOURCE_CHANGE_ORDER_PACKAGES: {},
                            RESOURCE_BUDGET_CHANGES: {}
                        }
                        event['line_items'].append(line_item)
                        by_id[str(change_item_id)] = line_item
                    resource = {
                        'id': resource_data['id'],
                        'status': resource_data['status'],
                        'custom_field': is_cf_present,
                    }
                    line_item[resource_name] = resource
                    event['last_updated'] = webhook_data['timestamp']
                return

            def update_line_item_resource(resource_name, resource_data):
                for change_event in updated_events.values():
                    if change_event.get('origin_id'):
                        continue
                    ce_event_id = str(change_event['event_id'])

                    for stored_line_item in change_event.get('line_items', []):
                        stored_resource = stored_line_item.get(resource_name, {})
                        if not stored_resource or stored_resource['id'] != resource_data['id']:
                            continue

                        stored_resource['status'] = resource_data['status']
                        stored_resource['custom_field'] = is_sync_custom_field_present(resource_data, custom_field_key)

                        updated_events[ce_event_id]['last_updated'] = webhook_data['timestamp']
                return

            validations = []
            is_CE_updated = False

            if webhook_data['resource_name'] == RESOURCE_CHANGE_EVENT:
                change_event = rail.result('fetch_change_event')
                ce_event_id = str(change_event['id'])
                origin_id = change_event['external_data']['origin_id'] \
                    if (change_event.get('external_data') or {}).get('origin_id') else None

                if change_event['status']['name'] != CE_STATUS_OPEN:
                    validations.append(f"Change event {ce_event_id} status is not {CE_STATUS_OPEN}")
                    return { 'validations': validations }

                ce_line_items = change_event.get('change_items', [])
                if not ce_line_items:
                    validations.append(f'Change event {ce_event_id} has no line items')
                    return { 'validations': validations }

                event = initialize_change_event_payload(
                    change_event['id'],
                    change_event['project_id']
                )
                if origin_id:
                    event['origin_id'] = origin_id

                # the change-event response carries only id/status,
                # so carry the sync-custom-field flag over from the previous state.
                prior_line_items = defaultdict(dict)
                existing_event = updated_events.get(ce_event_id)
                if existing_event:
                    event_line_items = existing_event.get('line_items')
                    for item in event_line_items:
                        prior_line_items[str(item['change_item_id'])][RESOURCE_BUDGET_CHANGES] = \
                            item.get(RESOURCE_BUDGET_CHANGES) or {}
                        prior_line_items[str(item['change_item_id'])][RESOURCE_CHANGE_ORDER_PACKAGES] = \
                            item.get(RESOURCE_CHANGE_ORDER_PACKAGES) or {}

                # rebuild line items fresh (prior figures already captured above)
                event['line_items'] = []
                for item in ce_line_items:
                    change_item_id = item.get('id')
                    budget_code = item['budget_code']['flat_code']
                    if not budget_code:
                        continue

                    # Check change order package status
                    cop = item.get('revenue_impact', {}).get('change_order_package')
                    if not cop:
                        # if a previously-ready CE loses its COP, revert it to pending
                        if ce_event_id in updated_events and is_resource_ready(updated_events[ce_event_id]):
                            updated_events[ce_event_id]['status'] = PENDING_APPROVAL
                            updated_events[ce_event_id]['last_updated'] = webhook_data['timestamp']
                            is_CE_updated = True

                    budget_change = item.get('budget_impact', {}).get('budget_change')
                    prior = prior_line_items.get(str(change_item_id), {})
                    prior_bc = prior.get(RESOURCE_BUDGET_CHANGES, {})
                    prior_cop = prior.get(RESOURCE_CHANGE_ORDER_PACKAGES, {})

                    line_item = {
                        'flat_code': budget_code,
                        'change_item_id': change_item_id,
                        RESOURCE_CHANGE_ORDER_PACKAGES: {
                            'id': cop['id'],
                            'status': cop['status'],
                            'custom_field': prior_cop.get('custom_field') or False
                        } if cop else {},
                        RESOURCE_BUDGET_CHANGES: {
                            'id': budget_change['id'],
                            'status': budget_change['status'],
                            'custom_field': prior_bc.get('custom_field') or False
                        } if budget_change else {}
                    }
                    event['line_items'].append(line_item)
                updated_events[ce_event_id] = event

            if webhook_data['resource_name'] == RESOURCE_CHANGE_ORDER_PACKAGES:
                cop_details = rail.result('fetch_change_order_package')
                if webhook_data['event_type'] == CREATE:
                    linked_change_event = rail.result('fetch_pco_linked_change_event')
                    if linked_change_event:
                        cop_id = cop_details['id']
                        targets = {}
                        for ci in linked_change_event.get('change_items', []):
                            cop_ref = ci.get('revenue_impact', {}).get('change_order_package') or {}
                            if cop_ref.get('id') != cop_id:
                                continue
                            targets[ci['id']] = {
                                'flat_code': ci['budget_code']['flat_code'],
                            }

                        event = initialize_change_event_payload(
                            linked_change_event['id'],
                            linked_change_event['project_id']
                        )
                        insert_resource_line_items(
                            event,
                            RESOURCE_CHANGE_ORDER_PACKAGES,
                            cop_details,
                            targets
                        )
                else:
                    update_line_item_resource(
                        RESOURCE_CHANGE_ORDER_PACKAGES,
                        cop_details
                    )

            if webhook_data['resource_name'] == RESOURCE_BUDGET_CHANGES:
                budget_change_details = rail.result('fetch_budget_change')
                adjustment_line_items = budget_change_details.get('adjustment_line_items') or []
                change_event_id = next(
                    (item['change_event_line_item']['event_id']
                     for item in adjustment_line_items if item.get('change_event_line_item')),
                    None
                )
                if change_event_id:
                    if webhook_data['event_type'] == CREATE:
                        targets = {
                            item['change_event_line_item_id']: {
                                'flat_code': (item.get('wbs_code') or {}).get('flat_code'),
                            }
                            for item in adjustment_line_items if item.get('change_event_line_item_id')
                        }

                        event = initialize_change_event_payload(
                            change_event_id,
                            webhook_data['project_id']
                        )
                        insert_resource_line_items(
                            event,
                            RESOURCE_BUDGET_CHANGES,
                            budget_change_details,
                            targets
                        )
                    else:
                        update_line_item_resource(
                            RESOURCE_BUDGET_CHANGES,
                            budget_change_details
                        )

            if webhook_data['resource_name'] != RESOURCE_CHANGE_EVENT or not is_CE_updated:
                for change_event in updated_events.values():
                    if change_event.get('origin_id'):
                        continue
                    line_items_list = change_event.get('line_items', [])

                    # Is valid budget change created on change event
                    any_line_has_budget_change = any(
                        is_resource_ready(line_item.get(RESOURCE_BUDGET_CHANGES, {}))
                        for line_item in line_items_list
                    )

                    if any_line_has_budget_change:
                        # Budget changes created: every line item needs both its COP and Budget Change approved (or carrying the sync custom field)
                        is_ready_to_sync = all(
                            is_resource_ready(line_item.get(RESOURCE_BUDGET_CHANGES, {})) and
                            is_resource_ready(line_item.get(RESOURCE_CHANGE_ORDER_PACKAGES, {}))
                            for line_item in line_items_list
                        )
                    else:
                        # No budget changes created on any line item: approve when all line items have a COP created + approved (or carrying the sync custom field)
                        is_ready_to_sync = all(
                            is_resource_ready(line_item.get(RESOURCE_CHANGE_ORDER_PACKAGES, {}))
                            for line_item in line_items_list
                        )

                    new_status = APPROVED if line_items_list and is_ready_to_sync else PENDING_APPROVAL
                    if new_status == APPROVED and change_event.get('status') != APPROVED:
                        change_event['last_updated'] = webhook_data['timestamp']
                    change_event['status'] = new_status

            return {
                'state_file': {
                    'events': updated_events,
                    'events_last_cleaned': events_last_cleaned
                },
                'validations': validations,
                'resource': webhook_data['resource_name']
            }

        build_event_payload = rail.PythonOperator(
            task_id='build_event_payload',
            python_callable=get_event_payload
        )

        should_update_events = rail.IfOperator(
            task_id='should_update_events',
            test="{{ result('build_event_payload').validations | length == 0 }}",
            yes_task='upload_events_to_s3',
            no_task='log_to_sumo'
        )

        upload_events_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_events_to_s3',
            source='{{ result("build_event_payload").state_file | tojson }}',
            bucket_name=config.s3_bucket_name,
            key_name=config.budget_revision_events_key,
            replace=True
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> should_process_webhook
        
        should_process_webhook >> rail.Label('No') >> log_to_sumo
        should_process_webhook >> rail.Label('Yes') >> extract_webhook
        extract_webhook >> download_existing_events >> load_existing_events >> should_fetch_custom_field

        should_fetch_custom_field >> rail.Label('Yes') >> fetch_custom_field >> is_change_event
        should_fetch_custom_field >> rail.Label('No') >> is_change_event

        is_change_event >> rail.Label('COP or BC') >> is_change_order_package
        is_change_event >> rail.Label('Change Event') >> fetch_change_event >> build_event_payload >> should_update_events

        is_change_order_package >> rail.Label('Budget Change') >> fetch_budget_change >> build_event_payload
        is_change_order_package >> rail.Label('Change Order Package') >> fetch_change_order_package >> should_fetch_linked_change_event

        should_fetch_linked_change_event >> rail.Label('Yes') >> fetch_pco_linked_change_event >> build_event_payload
        should_fetch_linked_change_event >> rail.Label('No') >> build_event_payload

        should_update_events >> rail.Label('Yes') >> upload_events_to_s3 >> log_to_sumo
        should_update_events >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
