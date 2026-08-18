import rail
from datetime import timedelta

from procore_ce_integration.change_orders_sync.utils.constants import (
    CE_STATUS_OPEN,
    RESOURCE_BUDGET_CHANGES,
    RESOURCE_CHANGE_ORDER_PACKAGES
)
from procore_ce_integration.change_orders_sync.utils.util import (
    parse_iso,
    is_resource_ready,
    is_sync_custom_field_present
)

def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.bulk_sync_dag_id,
        description='Procore to ComputerEase Change Orders Bulk Sync',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs_bulk_dag,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_change_order_packages',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_updated_events(response, dag_run):
            has_custom_field = is_sync_custom_field_present(
                response[0], dag_run.conf['custom_field_key']
            ) if response else None
            filtered = {}
            for item in response:
                if parse_iso(item['updated_at']) <= parse_iso(dag_run.conf['bulk_sync_start_time']):
                    continue
                filtered[item['id']] = item
            return {
                'items': filtered,
                'has_custom_field': has_custom_field
            }

        fetch_change_order_packages = rail.ProcoreApiOperator(
            task_id='fetch_change_order_packages',
            endpoint='/change_order_packages',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data_handler=lambda response, dag_run: get_updated_events(response, dag_run)
        )

        fetch_budget_changes = rail.ProcoreApiOperator(
            task_id='fetch_budget_changes',
            endpoint="/projects/{{ dag_run.conf.project_id }}/budget_changes",
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data_handler=lambda response, dag_run: get_updated_events(response, dag_run)
        )

        fetch_change_events = rail.ProcoreApiOperator(
            task_id='fetch_change_events',
            endpoint='/change_events',
            method='GET',
            version='1.1',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data={
                "filters": {
                    "prime_pco": {
                        "id": [ "with" ],
                        "operator": "includes"
                    }
                }
            },
            data_handler=lambda res: list(filter(lambda x: x.get('origin_id') is None, res))
        )

        def build_valid_events(dag_run):
            validations = []
            valid_change_events = []
            for change_event in rail.result('fetch_change_events'):
                ce_event_id = str(change_event['id'])
                origin_id = change_event['external_data']['origin_id'] \
                    if (change_event.get('external_data') or {}).get('origin_id') else None

                if origin_id:
                    validations.append({
                        'change_event': ce_event_id, 
                        'reason': f"Change event {ce_event_id} has already been synced with origin_id {origin_id}"
                    })
                    continue

                if change_event['status']['name'] != CE_STATUS_OPEN:
                    validations.append({
                        'change_event': ce_event_id,
                        'reason': f"Change event {ce_event_id} status is not {CE_STATUS_OPEN}"
                    })
                    continue

                ce_line_items = change_event.get('change_items', [])
                if not ce_line_items:
                    validations.append({
                        'change_event': ce_event_id,
                        'reason': f'Change event {ce_event_id} has no line items'
                    })
                    continue

                bc_id = (ce_line_items[0]['budget_impact']['budget_change'] or {}).get('id')
                budget_change = rail.result('fetch_budget_changes')['items'].get(str(bc_id)) if bc_id else None
                has_bc_custom_field = rail.result('fetch_budget_changes')['has_custom_field']

                cop_id = (ce_line_items[0]['revenue_impact']['change_order_package'] or {}).get('id')
                change_order_package = rail.result('fetch_change_order_packages')['items'].get(str(cop_id)) if cop_id else None
                has_cop_custom_field = rail.result('fetch_change_order_packages')['has_custom_field']

                ce_updated_at = parse_iso(change_event['updated_at'])
                sync_start_time = parse_iso(dag_run.conf['bulk_sync_start_time'])

                if budget_change is None and change_order_package is None and ce_updated_at < sync_start_time:
                    validations.append({
                        'change_event': ce_event_id,
                        'reason': f'Change event {ce_event_id} has older timestamp than bulk_sync_start_time'
                    })
                    continue

                event = {
                    'event_id': ce_event_id,
                    'project_id': change_event['project_id'],
                    'custom_field_key': dag_run.conf['custom_field_key'],
                    'line_items': []
                }

                line_items = []
                is_valid_change_event = True
                for item in ce_line_items:
                    budget_code = (item.get('budget_code') or {}).get('flat_code')
                    if not budget_code:
                        is_valid_change_event = False
                        break
                    cop = item.get('revenue_impact', {}).get('change_order_package')
                    if not cop:
                        validations.append({
                            'change_event': ce_event_id,
                            'reason': f"CE line item {item['id']} has no change order package"
                        })
                        is_valid_change_event = False
                        break

                    bc = item.get('budget_impact', {}).get('budget_change')
                    line_item = {
                        'flat_code': budget_code,
                        'change_item_id': item['id'],
                        RESOURCE_BUDGET_CHANGES: {
                            'id': bc['id'],
                            'status': bc['status'],
                            'custom_field': has_bc_custom_field
                        } if bc else {},
                        RESOURCE_CHANGE_ORDER_PACKAGES: {
                            'id': cop['id'],
                            'status': cop['status'],
                            'custom_field': has_cop_custom_field
                        }
                    }
                    line_items.append(line_item)

                if line_items and is_valid_change_event:
                    event['line_items'].extend(line_items)
                    valid_change_events.append(event)

            return {
                'valid_events': valid_change_events,
                'validations': validations
            }

        filter_updated_events = rail.PythonOperator(
            task_id='filter_updated_events',
            python_callable=build_valid_events
        )

        def build_events_to_sync(dag_run):
            valid_change_events = rail.result('filter_updated_events')['valid_events']

            events_to_sync = []
            for event in valid_change_events:
                line_items_list = event['line_items']

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

                if is_ready_to_sync:
                    events_to_sync.append({
                        'event_id': int(event['event_id']),
                        'job_code': dag_run.conf.get('job_code'),
                        'project_id': int(dag_run.conf['project_id']),
                        'custom_field_key': dag_run.conf['custom_field_key'],
                        'should_sync_budget': any_line_has_budget_change
                    })

            return events_to_sync

        process_valid_events = rail.PythonOperator(
            task_id='process_valid_events',
            python_callable=build_events_to_sync
        )

        has_valid_events = rail.IfOperator(
            task_id='has_valid_events',
            test="{{ result('process_valid_events') | length > 0 }}",
            yes_task='trigger_child_dags',
            no_task='catch_error'
        )


        trigger_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_dags',
            trigger_dag_id=config.child_dag_id,
            items=lambda: rail.result('process_valid_events'),
            conf=lambda item, dag_run: {
                **item,
                'cost_type_mapping': dag_run.conf['cost_type_mapping']
            }
        )

        wait_for_child_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_completion',
            dag_runs="{{ result('trigger_child_dags') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties={
                'project_id': '{{ dag_run.conf.project_id }}',
                'job_code': '{{ dag_run.conf.job_code }}',
                'error': '{{ get_error_message() }}'
            }
        )

        batch_task >> catch_error
        batch_task >> fetch_change_order_packages >> fetch_budget_changes >> fetch_change_events
        fetch_change_events >> filter_updated_events >> process_valid_events >> has_valid_events

        has_valid_events >> rail.Label('No') >> catch_error
        has_valid_events >> rail.Label('Yes') >> trigger_child_dags >> wait_for_child_completion >> catch_error

    return dag


rail.for_each_instance(create_dag_instance)
