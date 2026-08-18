from datetime import timedelta
import rail
from ce_procore_integration.subcontract_sync_v2.utils.util import build_flat_code


def create_dag_instance(config):  # pylint: disable= too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.subcontract_line_items_child_dag_id,
        description='Computerease to Procore Subcontract Line Items Sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id,
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_existing_line_items',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Fetch existing line items from Procore to identify items to delete
        fetch_existing_line_items = rail.ProcoreApiOperator(
            task_id='fetch_existing_line_items',
            endpoint=lambda dag_run: f'/work_order_contracts/{dag_run.conf["subcontract_id"]}/line_items',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf["project_id"] }}'
            },
            data_handler=lambda line_items: {
                item.get('origin_id'): item.get('id')
                for item in line_items
                if item.get('origin_id') and item.get('origin_id').startswith('CE_')
            } if line_items else {}
        )

        def prepare_line_items_data(dag_run):
            subcontract_data = dag_run.conf['subcontract_data']
            wbs_codes_map = dag_run.conf.get('wbs_codes_lookup') or {}
            # CE stores cost type as ID (e.g., 1, 2) but Procore uses reference (e.g., 'L', 'M')
            # This map converts CE cost type ID to reference for Procore WBS code lookup
            ce_cost_type_map = dag_run.conf.get('ce_cost_type_map', {})

            line_items_with_wbs = []
            line_items_with_wbs_errors = []

            for item in subcontract_data.get('subcontract_item', []):
                phase_code = item.get('phase_code', '').strip()
                category_code = item.get('category_code', '').strip()

                # Convert CE cost type ID to Procore reference
                cost_type_id = str(item.get('costtype', '')).strip()
                cost_type = ce_cost_type_map.get(cost_type_id, '')

                flat_code = build_flat_code(phase_code, category_code, cost_type)

                wbs_code_id = wbs_codes_map.get(flat_code)

                item_data = {
                    'unique_id': item.get('unique_id'),
                    'description': item.get('description', ''),
                    'amount': str(item.get('amount', 0)),
                    'quantity': str(item.get('units', 0)),
                    'unit_price': str(item.get('unit_price', 0)),
                    'flat_code': flat_code,
                }

                if isinstance(wbs_code_id, int):
                    item_data['wbs_code_id'] = wbs_code_id
                    line_items_with_wbs.append(item_data)
                else:
                    error_msg = wbs_code_id if isinstance(wbs_code_id, str) else f"WBS code not found for {flat_code}"
                    item_data['error_message'] = error_msg
                    line_items_with_wbs_errors.append(item_data)

            return {
                'line_items_with_wbs': line_items_with_wbs,
                'line_items_with_wbs_errors': line_items_with_wbs_errors
            }

        prepare_line_items = rail.PythonOperator(
            task_id='prepare_line_items',
            python_callable=prepare_line_items_data
        )

        def create_line_item_payload(item, origin_id, wbs_code_id):
            amount = float(item.get('amount'))
            quantity = float(item.get('quantity')) or 1
            unit_price = amount / quantity
            return {
                "origin_id": origin_id,
                "description": item['description'],
                "amount": str(amount),
                "quantity": str(quantity),
                "unit_cost": str(unit_price),
                "wbs_code_id": wbs_code_id
            }

        def build_final_line_items_payload(dag_run):
            subcontract_data = dag_run.conf['subcontract_data']
            prepared_items = rail.result('prepare_line_items')
            existing_line_items = rail.result('fetch_existing_line_items', {})

            line_items_updates = []
            line_items_to_delete = []
            wbs_creation_errors = []

            # Track CE origin_ids to identify items to delete
            ce_origin_ids = set()

            # Items that resolved to a valid wbs_code_id
            for item in prepared_items['line_items_with_wbs']:
                origin_id = f"CE_{subcontract_data.get('code')}_{item['unique_id']}"
                ce_origin_ids.add(origin_id)
                line_items_updates.append(create_line_item_payload(item, origin_id, item['wbs_code_id']))

            # Items where WBS lookup failed (logged, not synced)
            for item in prepared_items.get('line_items_with_wbs_errors', []):
                wbs_creation_errors.append({
                    'flat_code': item['flat_code'],
                    'error_message': f"Line item for {item['flat_code']} not synced - {item.get('error_message', '')}"
                })

            # Identify line items to delete (exist in Procore but not in CE)
            for existing_origin_id, existing_id in existing_line_items.items():
                if existing_origin_id not in ce_origin_ids:
                    line_items_to_delete.append({
                        "id": existing_id
                    })

            return {
                "updates": line_items_updates,
                "line_items_to_delete": line_items_to_delete,
                "wbs_creation_errors": wbs_creation_errors
            }

        build_final_payload = rail.PythonOperator(
            task_id='build_final_payload',
            python_callable=build_final_line_items_payload
        )

        if_wbs_creation_errors = rail.IfOperator(
            task_id='if_wbs_creation_errors',
            test='{{ result("build_final_payload").wbs_creation_errors | length > 0 }}',
            yes_task='log_wbs_creation_errors',
            no_task='if_line_items_to_delete'
        )

        log_wbs_creation_errors = rail.WriteLogOperator(
            task_id='log_wbs_creation_errors',
            message='na',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'subcontract_code': dag_run.conf['subcontract_data'].get('code', ''),
                    'vendor_code': dag_run.conf['subcontract_data'].get('vendor_code', ''),
                    'job_code': dag_run.conf['subcontract_data'].get('job_code', ''),
                    'flat_code': err['flat_code'],
                    'error_message': err['error_message']
                }
                for err in rail.result('build_final_payload').get('wbs_creation_errors', [])
            ],
            properties=lambda item: item
        )

        if_line_items_to_delete = rail.IfOperator(
            task_id='if_line_items_to_delete',
            test=lambda: len(rail.result("build_final_payload")[
                             'line_items_to_delete']) > 0,
            yes_task='trigger_line_items_deletion',
            no_task='if_valid_line_items_to_sync'
        )

        trigger_line_items_deletion = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_line_items_deletion',
            items='{{ result("build_final_payload")["line_items_to_delete"] | to_json }}',
            trigger_dag_id=config.subcontract_line_items_deletion_child_dag_id,
            execution_timeout=timedelta(minutes=30),
            conf=lambda item, dag_run: {
                'subcontract_id': dag_run.conf['subcontract_id'],
                'project_id': dag_run.conf['project_id'],
                'line_item_id': item['id'],
                'subcontract_code': dag_run.conf['subcontract_data'].get('code', ''),
                'vendor_code': dag_run.conf['subcontract_data'].get('vendor_code', ''),
                'job_code': dag_run.conf['subcontract_data'].get('job_code', '')
            }
        )

        wait_for_deletions = rail.WaitForDagRunsSensor(
            task_id='wait_for_deletions',
            dag_runs='{{ result("trigger_line_items_deletion") }}',
            execution_timeout=timedelta(minutes=30)
        )

        if_valid_line_items_to_sync = rail.IfOperator(
            task_id='if_valid_line_items_to_sync',
            test=lambda: len(rail.result(
                "build_final_payload")['updates']) > 0,
            yes_task='sync_line_items_to_procore',
            no_task='catch_error'
        )

        sync_line_items_to_procore = rail.ProcoreApiOperator(
            task_id='sync_line_items_to_procore',
            endpoint=lambda dag_run: f'/work_order_contracts/{dag_run.conf["subcontract_id"]}/line_items/sync',
            method='PATCH',
            data=lambda: {'updates': rail.result(
                'build_final_payload')['updates']},
            query_params={
                'project_id': '{{ dag_run.conf["project_id"] }}',
                'run_configurable_validations': 'false'
            }
        )

        check_if_sync_has_errors = rail.IfOperator(
            task_id='check_if_sync_has_errors',
            test='{{ result("sync_line_items_to_procore").errors | length > 0 }}',
            yes_task='log_sync_failure',
            no_task='catch_error'
        )

        def get_error_message(dag_run, error_object):
            """
            Extract error message from Procore API response.
            Uses conf-passed wbs_codes_lookup (only int values are valid WBS ids) to reverse-map id → flat_code.
            """
            try:
                wbs_codes_map = dag_run.conf.get('wbs_codes_lookup') or {}
                wbs_codes_reverse_map = {v: k for k, v in wbs_codes_map.items() if isinstance(v, int)}
                errors = error_object.get('errors', {})
                if not errors or not isinstance(errors, dict):
                    return ""
                messages = []
                for key, msgs in errors.items():
                    if isinstance(msgs, list):
                        msg_str = ", ".join(str(m) for m in msgs)
                    else:
                        msg_str = str(msgs)
                    messages.append(f"{key}: {msg_str}")
                return f"SOV not synced for {wbs_codes_reverse_map.get(error_object.get('wbs_code_id'))} due to - " + "; ".join(messages)
            except Exception as e:
                return f"Error parsing error message: {str(e)}"

        log_sync_failure = rail.WriteLogOperator(
            task_id='log_sync_failure',
            message='na',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'subcontract_code': dag_run.conf['subcontract_data'].get('code', ''),
                    'vendor_code': dag_run.conf['subcontract_data'].get('vendor_code', ''),
                    'job_code': dag_run.conf['subcontract_data'].get('job_code', ''),
                    'error_message': get_error_message(dag_run, err)
                }
                for err in rail.result('sync_line_items_to_procore').get('errors', [])
            ],
            properties=lambda item: item
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'subcontract_code': dag_run.conf['subcontract_data'].get('code', ''),
                'vendor_code': dag_run.conf['subcontract_data'].get('vendor_code', ''),
                'job_code': dag_run.conf['subcontract_data'].get('job_code', ''),
                'error_message': "Subcontract line items not synced - {{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> catch_error
        batch_task >> fetch_existing_line_items >> prepare_line_items >> build_final_payload >> if_wbs_creation_errors

        if_wbs_creation_errors >> rail.Label(
            'Yes') >> log_wbs_creation_errors >> if_line_items_to_delete
        if_wbs_creation_errors >> rail.Label(
            'No') >> if_line_items_to_delete

        if_line_items_to_delete >> rail.Label(
            'Yes') >> trigger_line_items_deletion >> wait_for_deletions >> if_valid_line_items_to_sync
        if_line_items_to_delete >> rail.Label(
            'No') >> if_valid_line_items_to_sync

        if_valid_line_items_to_sync >> rail.Label(
            'Yes') >> sync_line_items_to_procore
        if_valid_line_items_to_sync >> rail.Label('No') >> catch_error

        sync_line_items_to_procore >> check_if_sync_has_errors

        check_if_sync_has_errors >> rail.Label(
            'Yes') >> log_sync_failure >> catch_error
        check_if_sync_has_errors >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
