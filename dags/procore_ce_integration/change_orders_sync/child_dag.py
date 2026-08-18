import io
import rail
import base64
import zipfile
from datetime import timedelta
from collections import defaultdict

from procore_ce_integration.initial_setup_sync.shared_utils import (
    build_import_file_description,
    normalize_ce_identifier,
    parse_wbs_flat_code
)
from procore_ce_integration.change_orders_sync.utils.constants import (
    APPROVED,
    RESOURCE_CHANGE_EVENT,
    WBSType
)
from procore_ce_integration.change_orders_sync.utils.util import generate_budget_revision_xml


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='Procore to ComputerEase Change Order Sync - Sync Change event to CE',
        max_active_runs=config.max_active_runs_child_dag,
        integration_type='generic',
        company_key=config.instance,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_change_event',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )


        def get_change_event(response):
            change_event = response[0] if response else {}
            change_items = change_event.get('change_items') or []

            first_cop = next(
                (
                    item['revenue_impact']['change_order_package']
                    for item in change_items
                    if item.get('revenue_impact', {}).get('change_order_package')
                ),
                {}
            )
            first_bc = next(
                (
                    item['budget_impact']['budget_change']
                    for item in change_items
                    if item.get('budget_impact', {}).get('budget_change')
                ),
                {}
            )
            return {
                'change_event': change_event,
                'budget_change_id': first_bc.get('id'),
                'budget_change_status': first_bc.get('status'),
                'change_order_package_id': first_cop.get('id'),
                'change_order_package_number': first_cop.get('number'),
                'change_order_package_status': first_cop.get('status')
            }
        fetch_change_event = rail.ProcoreApiOperator(
            task_id='fetch_change_event',
            endpoint="/change_events/{{ dag_run.conf.event_id }}",
            method='GET',
            version='1.1',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=get_change_event
        )

        is_erp_synced = rail.IfOperator(
            task_id='is_erp_synced',
            test=lambda: (rail.result('fetch_change_event')['change_event'].get('external_data') or {}).get('origin_id'),
            yes_task='catch_error',
            no_task='fetch_wbs_cost_codes'
        )

        fetch_wbs_cost_codes = rail.ProcoreApiOperator(
            task_id='fetch_wbs_cost_codes',
            endpoint="/cost_codes",
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=lambda items: {
                item['id']: {
                    'code': item.get('code'),
                    'parent': item.get('parent')
                } for item in (items or [])
            }
        )


        def get_ce_job_details(response):
            if not response.get('data'):
                raise ValueError(
                    "Job not found in ComputerEase — sync job first before syncing change orders"
                )
            return {
                'wbs_type': response['data'][0].get('wbs_type'),
                'job_found': True
            }
        fetch_ce_job_details = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_job_details',
            endpoint='/catalog/job',
            request_method='GET',
            query_params={
                'code': '{{ dag_run.conf.job_code }}'
            },
            data_handler=get_ce_job_details
        )

        should_fetch_budget_change = rail.IfOperator(
            task_id='should_fetch_budget_change',
            test='{{ dag_run.conf.should_sync_budget | is_truthy }}',
            yes_task='fetch_budget_change',
            no_task='fetch_change_order_package'
        )

        fetch_budget_change = rail.ProcoreApiOperator(
            task_id='fetch_budget_change',
            endpoint="/projects/{{ dag_run.conf.project_id }}/budget_changes/{{ result('fetch_change_event').budget_change_id }}",
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=lambda res: res[0].get('data') if res and len(res) > 0 else {}
        )

        fetch_change_order_package = rail.ProcoreApiOperator(
            task_id='fetch_change_order_package',
            endpoint="/change_order_packages/{{ result('fetch_change_event').change_order_package_id }}",
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=lambda res: res[0] if res and len(res) > 0 else {}
        )

        def extract_revenue_and_budget_data(dag_run):
            def to_float(value):
                return float(value or 0)

            should_sync_budget = dag_run.conf.get('should_sync_budget', True)
            wbs_type = rail.result('fetch_ce_job_details').get('wbs_type')

            def get_phase_key(phase_num, cat_num):
                if wbs_type == WBSType.JOB_CAT:
                    return cat_num
                if cat_num:
                    return f"{phase_num}-{cat_num}"
                return phase_num

            change_event = rail.result("fetch_change_event")["change_event"]
            change_order_package_number = rail.result("fetch_change_event")["change_order_package_number"]
            change_order_package_status = rail.result("fetch_change_event")["change_order_package_status"]
            change_items = change_event.get("change_items") or []
            cost_codes_map = rail.result("fetch_wbs_cost_codes") or {}

            # The change event's embedded impact/estimate is the value the resource was *created* with
            # and does not reflect later edits, so COP and BC are fetched from Procore for revenue and budget values.

            # Build per-line quantity/unit_cost from freshly fetched COP
            cop_details = rail.result('fetch_change_order_package') or {}
            cop_lines_by_id = {li['id']: li for li in (cop_details.get('line_items') or [])}

            # Build per-line quantity/unit_cost from freshly fetched BC (if syncing budget)
            bc_details = rail.result('fetch_budget_change') or {}
            bc_lines_by_id = {
                str(adj['change_event_line_item_id']): adj
                for adj in (bc_details.get('adjustment_line_items') or [])
                if adj.get('change_event_line_item_id') is not None
            } if bc_details else {}

            event_metadata = {
                "number": change_event.get("number", ""),
                "title": change_event.get("title", ""),
                "description": change_event.get("description", ""),
                "created_at": change_event.get("created_at", ""),
                "updated_at": change_event.get("updated_at", ""),
                "change_order_num": change_order_package_number
            }
            if change_order_package_status == APPROVED:
                event_metadata["approved"] = True
                event_metadata["approved_by"] = (change_event.get("created_by") or {}).get("name", "")

            # Combine by flat code
            combined_by_flat_code = defaultdict(
                lambda: {
                    "flat_code": "",
                    "cost_code_id": "",
                    "revenue": 0.0,
                    "budget_amount": 0.0,
                    "budget_quantity": 0.0,
                    "budget_unit_cost": 0.0,
                }
            )
            for item in change_items:
                budget_code_obj = item.get("budget_code") or {}
                flat_code = budget_code_obj.get("flat_code")
                if not flat_code:
                    continue

                cost_code_item = next(
                    (si for si in budget_code_obj.get("segment_items", [])
                     if (si.get("segment") or {}).get("type") == "cost_code"),
                    None
                )
                cost_code_id = cost_code_item.get("id") if cost_code_item else None

                # Revenue from freshly fetched COP line (matched by line_item.id)
                cop_ref = (item.get('revenue_impact') or {}).get('change_order_package') or {}
                cop_line_id = (cop_ref.get('line_item') or {}).get('id')
                cop_line = cop_lines_by_id.get(cop_line_id) or {}
                revenue_amount = to_float(cop_line.get("quantity")) * to_float(cop_line.get("unit_cost"))

                # Budget from freshly fetched BC adjustment (matched by change_event_line_item_id)
                change_item_id = str(item.get("id"))
                bc_line_item = bc_lines_by_id.get(change_item_id) or {}
                budget_quantity = to_float(bc_line_item.get("quantity"))
                budget_unit_cost = to_float(bc_line_item.get("unit_cost"))
                budget_amount = budget_quantity * budget_unit_cost

                entry = combined_by_flat_code[flat_code]
                entry["flat_code"] = flat_code
                entry["cost_code_id"] = cost_code_id
                entry["revenue"] += revenue_amount
                entry["budget_amount"] += budget_amount
                entry["budget_quantity"] = budget_quantity
                entry["budget_unit_cost"] = budget_unit_cost

            # Group by phase
            phase_groups = defaultdict(
                lambda: {
                    "phase_num": "",
                    "cat_num": "",
                    "revenue": 0.0,
                    "cost_types": defaultdict(
                        lambda: {
                            "amount": 0.0,
                            "quantity": 0.0,
                            "unit_cost": 0.0,
                        }
                    ),
                }
            )

            for flat_code, data in combined_by_flat_code.items():
                cost_code_details = cost_codes_map.get(str(data["cost_code_id"]))
                phase_num, cat_num, cost_type_ref = parse_wbs_flat_code(
                    flat_code, cost_code_details, wbs_type
                )

                phase_key = get_phase_key(phase_num, cat_num)
                if not phase_key:
                    continue

                phase_entry = phase_groups[phase_key]
                phase_entry["phase_num"] = phase_num
                phase_entry["cat_num"] = cat_num
                phase_entry["revenue"] += data["revenue"]

                if should_sync_budget and cost_type_ref:
                    cost_entry = phase_entry["cost_types"][cost_type_ref]
                    cost_entry["amount"] += data["budget_amount"]
                    cost_entry["quantity"] = data["budget_quantity"]
                    cost_entry["unit_cost"] = data["budget_unit_cost"]

            # Convert nested defaultdicts to regular dicts (important for serialization)
            for phase in phase_groups.values():
                phase["cost_types"] = dict(phase["cost_types"])

            return {
                "phase_groups": dict(phase_groups),
                "event_metadata": event_metadata
            }

        combine_data = rail.PythonOperator(
            task_id='combine_data',
            python_callable=extract_revenue_and_budget_data
        )


        def generate_rfc_xml(dag_run):
            combine_data_result = rail.result('combine_data')
            phase_groups_data = combine_data_result['phase_groups']
            cost_type_mapping = dag_run.conf['cost_type_mapping']
            event_info = combine_data_result['event_metadata']

            try:
                xml_string = generate_budget_revision_xml(
                    phase_groups=phase_groups_data,
                    job_code=dag_run.conf['job_code'],
                    config=config,
                    cost_type_map=cost_type_mapping,
                    event_metadata=event_info
                )
                return {
                    'success': True,
                    'xml_content': xml_string
                }
            except Exception as e:
                return {
                    'success': False,
                    'error': str(e)
                }

        transform_to_rfc = rail.PythonOperator(
            task_id='transform_to_rfc',
            python_callable=generate_rfc_xml
        )

        is_xml_generated = rail.IfOperator(
            task_id='is_xml_generated',
            test="{{ result('transform_to_rfc').success }}",
            yes_task='zip_and_encode_xml',
            no_task='log_transform_error'
        )

        log_transform_error = rail.WriteLogOperator(
            task_id='log_transform_error',
            message='Failed to generate RFC XML',
            severity='Error/Exception',
            properties={
                'project_id': '{{ dag_run.conf.project_id }}',
                'event_id': '{{ dag_run.conf.event_id }}',
                'error': "{{ result('transform_to_rfc').get('error', 'Unknown error') }}"
            }
        )

        def create_zip_and_encode(dag_run):
            project_id = dag_run.conf['project_id']
            event_info = rail.result('combine_data')['event_metadata']
            event_number = event_info.get('number', project_id)

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr(
                    f'budget_revision_{event_number}.xml',
                    rail.result('transform_to_rfc')['xml_content']
                )
            zip_bytes = zip_buffer.getvalue()
            base64_encoded = base64.b64encode(zip_bytes).decode('utf-8')
            description = build_import_file_description(RESOURCE_CHANGE_EVENT, event_number)[:config.IMPORT_FILE_DESCRIPTION_LIMIT]
            return {
                'import_data': base64_encoded,
                'description': description
            }

        zip_and_encode_xml = rail.PythonOperator(
            task_id='zip_and_encode_xml',
            python_callable=create_zip_and_encode
        )

        send_to_computerease, import_sync_finish = rail.computerease_import_sync(
            group_id='send_to_computerease',
            import_type='Job Costing RFC',
            description=lambda: rail.result('zip_and_encode_xml')['description'],
            import_data=lambda: rail.result('zip_and_encode_xml')['import_data']
        )

        should_update_cop_custom_field = rail.IfOperator(
            task_id='should_update_cop_custom_field',
            test="{{ dag_run.conf.custom_field_key | is_truthy }}",
            yes_task='update_cop_custom_field',
            no_task='should_mark_CE_erp_synced'
        )

        def get_sync_time():
            data = (
                rail.result('send_to_computerease.create_import_file') or
                rail.result('send_to_computerease.update_import_file')
            )
            if data and data.get('data'):
                return data['data'].get('updated_at')
            return None

        update_cop_custom_field = rail.ProcoreApiOperator(
            task_id='update_cop_custom_field',
            endpoint="/projects/{{ dag_run.conf.project_id }}/prime_change_order_batches/{{ result('fetch_change_event').change_order_package_id }}",
            method='PATCH',
            query_params={ 'run_configurable_validations': 'true' },
            data=lambda dag_run: {
                dag_run.conf['custom_field_key']: get_sync_time()
            }
        )

        should_update_bc_custom_field = rail.IfOperator(
            task_id='should_update_bc_custom_field',
            test='{{ dag_run.conf.should_sync_budget | is_truthy }}',
            yes_task='update_bc_custom_field',
            no_task='should_mark_CE_erp_synced'
        )

        update_bc_custom_field = rail.ProcoreApiOperator(
            task_id='update_bc_custom_field',
            endpoint="/projects/{{ dag_run.conf.project_id }}/budget_changes/{{ result('fetch_change_event').budget_change_id }}",
            method='PATCH',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data=lambda dag_run: {
                'status': 'draft',
                'custom_fields': {
                    dag_run.conf['custom_field_key']: get_sync_time()
                }
            }
        )

        should_reapprove_budget_change = rail.IfOperator(
            task_id='should_reapprove_budget_change',
            test=lambda: rail.result('fetch_change_event')['budget_change_status'] == APPROVED,
            yes_task='reapprove_budget_change',
            no_task='should_mark_CE_erp_synced'
        )

        reapprove_budget_change = rail.ProcoreApiOperator(
            task_id='reapprove_budget_change',
            endpoint="/projects/{{ dag_run.conf.project_id }}/budget_changes/{{ result('fetch_change_event').budget_change_id }}",
            method='PATCH',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data={ 'status': APPROVED }
        )

        def check_if_bc_and_cop_is_approved(dag_run):
            change_event = rail.result("fetch_change_event")
            if change_event["change_order_package_status"] != APPROVED:
                return False
            if dag_run.conf['should_sync_budget']:
                if change_event["budget_change_status"] != APPROVED:
                    return False
            return True
        should_mark_CE_erp_synced = rail.IfOperator(
            task_id='should_mark_CE_erp_synced',
            test=check_if_bc_and_cop_is_approved,
            yes_task='should_defer_origin_id',
            no_task='catch_error'
        )

        # Defer: enqueue the link for the mark-erp DAG instead of PATCHing origin_id now.
        should_defer_origin_id = rail.IfOperator(
            task_id='should_defer_origin_id',
            test=lambda: config.defer_origin_id_until_accepted,
            yes_task='is_already_deferred',
            no_task='set_change_event_origin_id'
        )

        is_already_deferred = rail.IfOperator(
            task_id='is_already_deferred',
            test=lambda: rail.result('send_to_computerease.get_import_file_id'),
            yes_task='catch_error',
            no_task='build_pending_rows'
        )

        def get_ce_import_uuid():
            import_uuid = rail.result('send_to_computerease.get_import_file_id')
            if not import_uuid:
                created = rail.result('send_to_computerease.create_import_file') or {}
                import_uuid = (created.get('data') or {}).get('uuid', '')
            return import_uuid

        def build_pending_rows(dag_run):
            change_event = rail.result('fetch_change_event')
            change_event_id = change_event['change_event']['id']
            cop_id = change_event['change_order_package_id']
            # Same origin_id formula as set_change_event_origin_id.
            origin_id = f"CE_{normalize_ce_identifier(dag_run.conf['job_code'])}_COP_{cop_id}"
            return [{
                'change_event_id': str(change_event_id),
                'project_id': str(dag_run.conf['project_id']),
                'origin_id': origin_id,
                'import_uuid': get_ce_import_uuid(),
                'queued_at': rail.render_template('{{ current_time() }}')
            }]

        build_pending_rows_task = rail.PythonOperator(
            task_id='build_pending_rows',
            python_callable=build_pending_rows
        )

        # Upsert keyed on change_event_id so a re-queue overwrites the stale pending row.
        enqueue_pending = rail.S3UpsertCollectionOperator(
            task_id='enqueue_pending',
            integration=config.s3_collection['integration'],
            customer=config.instance,
            collection_name=config.origin_id_update_table['name'],
            key_columns=config.origin_id_update_table['unique_columns'],
            rows=build_pending_rows_task.output
        )

        set_change_event_origin_id = rail.ProcoreApiOperator(
            task_id='set_change_event_origin_id',
            endpoint="/change_events/{{ result('fetch_change_event').change_event.id }}",
            method='PATCH',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data=lambda dag_run: {
                "project_id": dag_run.conf["project_id"],
                "change_event": {
                    "origin_id": f"CE_{normalize_ce_identifier(dag_run.conf['job_code'])}_COP_{rail.result('fetch_change_event')['change_order_package_id']}"
                }
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties={
                'project_id': '{{ dag_run.conf.project_id }}',
                'event_id': '{{ dag_run.conf.event_id }}',
                'error': '{{ get_error_message() }}'
            }
        )

        batch_task >> catch_error
        batch_task >> fetch_change_event >> is_erp_synced
        
        is_erp_synced >> rail.Label('Yes') >> catch_error
        is_erp_synced >> rail.Label('No') >> fetch_wbs_cost_codes >> fetch_ce_job_details >> should_fetch_budget_change

        should_fetch_budget_change >> rail.Label('Yes') >> fetch_budget_change >> fetch_change_order_package >> combine_data
        should_fetch_budget_change >> rail.Label('No') >> fetch_change_order_package >> combine_data

        combine_data >> transform_to_rfc >> is_xml_generated

        is_xml_generated >> rail.Label('No') >> log_transform_error >> catch_error
        is_xml_generated >> rail.Label('Yes') >> zip_and_encode_xml >> send_to_computerease
        import_sync_finish >> should_update_cop_custom_field
        
        should_update_cop_custom_field >> rail.Label('No') >> should_mark_CE_erp_synced
        should_update_cop_custom_field >> rail.Label('yes') >> update_cop_custom_field >> should_update_bc_custom_field

        should_update_bc_custom_field >> rail.Label('No') >> should_mark_CE_erp_synced
        should_update_bc_custom_field >> rail.Label('Yes') >> update_bc_custom_field >> should_reapprove_budget_change

        should_reapprove_budget_change >> rail.Label('No') >> should_mark_CE_erp_synced
        should_reapprove_budget_change >> rail.Label('Yes') >> reapprove_budget_change >> should_mark_CE_erp_synced

        should_mark_CE_erp_synced >> rail.Label('Yes') >> should_defer_origin_id
        should_mark_CE_erp_synced >> rail.Label('No') >> catch_error

        should_defer_origin_id >> rail.Label('No') >> set_change_event_origin_id >> catch_error
        should_defer_origin_id >> rail.Label('Yes') >> is_already_deferred

        is_already_deferred >> rail.Label('No') >> build_pending_rows_task >> enqueue_pending >> catch_error
        is_already_deferred >> rail.Label('Yes') >> catch_error

    return dag


rail.for_each_instance(create_dag_instance)
