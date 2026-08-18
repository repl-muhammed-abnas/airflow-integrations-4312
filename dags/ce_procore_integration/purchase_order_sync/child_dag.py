import rail
from datetime import timedelta
from ce_procore_integration.purchase_order_sync.utils.constants import ProcorePurchaseOrderStatus
from ce_procore_integration.purchase_order_sync.utils.util import build_flat_code, convert_date
from ce_procore_integration.util_dags.utils import normalize_ce_identifier


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='Purchase Order Sync Child - Process Individual PO to Procore',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_wbs_codes',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def extract_required_wbs_codes(dag_run):
            """Returns unique required WBS codes as items the WBS code creator
            util DAG expects: {flat_code, phase_code, category_code, cost_type}."""
            batch = dag_run.conf['purchase_order_batch']
            purchase_orders = batch['purchase_orders']

            required = {}
            for po in purchase_orders:
                for item in po['line_items']:
                    phase = item['phase_code']
                    category = item['category_code']
                    cost_type = item['cost_type']

                    flat_code = build_flat_code(phase, category, cost_type)
                    if flat_code and flat_code not in required:
                        required[flat_code] = {
                            'flat_code': flat_code,
                            'phase_code': phase,
                            'category_code': category,
                            'cost_type': cost_type
                        }

            return list(required.values())

        def get_flat_codes(response, dag_run):
            required_items = extract_required_wbs_codes(dag_run)

            existing_wbs = list(
                map(lambda wbs: {'id': wbs['id'], 'flat_code': normalize_ce_identifier(wbs['flat_code'])}, response)
            )
            existing_wbs_codes = set(
                map(lambda wbs: wbs['flat_code'], existing_wbs)
            )
            missing = [
                item for item in required_items
                if item['flat_code'] not in existing_wbs_codes
            ]
            cost_code_segment_id = next(
                (
                    item['segment']['id']
                    for wbs in response
                    for item in wbs.get('segment_items', [])
                    if item.get('segment', {}).get('name') == config.cost_code_segment_name
                    and item.get('segment', {}).get('type') == config.cost_code_segment_type
                ),
                None
            )
            cost_type_segment_id = next(
                (
                    item['segment']['id']
                    for wbs in response
                    for item in wbs.get('segment_items', [])
                    if item.get('segment', {}).get('name') == config.cost_type_name
                    and item.get('segment', {}).get('type') == config.cost_type_type
                ),
                None
            )
            return {
                'existing': existing_wbs,
                'missing': missing,
                'cost_code_segment_id': cost_code_segment_id,
                'cost_type_segment_id': cost_type_segment_id
            }

        get_wbs_codes = rail.ProcoreApiOperator(
            task_id='get_wbs_codes',
            endpoint=lambda dag_run: f"/projects/{dag_run.conf['purchase_order_batch']['project_id']}/work_breakdown_structure/wbs_codes",
            method='GET',
            data_handler=lambda response, dag_run: get_flat_codes(response, dag_run)
        )

        has_missing_codes = rail.IfOperator(
            task_id='has_missing_codes',
            test='{{ result("get_wbs_codes").missing | length > 0 }}',
            yes_task='trigger_wbs_code_creation',
            no_task='syncable_purchase_orders'
        )

        trigger_wbs_code_creation = rail.TriggerDagRunOperator(
            task_id='trigger_wbs_code_creation',
            trigger_dag_id=config.wbs_code_creator_dag_id,
            conf=lambda dag_run: {
                'project_id': dag_run.conf['purchase_order_batch']['project_id'],
                'wbs_codes_to_create': rail.result('get_wbs_codes')['missing'],
                'cost_code_segment_id': rail.result('get_wbs_codes')['cost_code_segment_id'],
                'cost_type_segment_id': rail.result('get_wbs_codes')['cost_type_segment_id']
            }
        )

        wait_for_wbs_code_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_wbs_code_creation',
            dag_runs='{{ result("trigger_wbs_code_creation") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_wbs_creation_results = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_wbs_creation_results',
            dag_runs='{{ result("trigger_wbs_code_creation") }}',
            dagrun_task_id='compile_results'
        )

        def get_created_wbs_results():
            """Returns the list of {flat_code: id_or_error}"""
            try:
                results = rail.result('gather_wbs_creation_results')
            except Exception:  # pylint: disable=broad-except
                results = None
            return [r for r in (results or []) if r and isinstance(r, dict)]

        def build_syncable_purchase_orders(dag_run):
            """Builds the WBS code mapping and splits POs into syncable vs skipped.

            The util DAG returns an integer id per flat_code on success and an
            error message string on failure. Successful codes (existing + newly
            created) form the mapping; a PO is skipped when any of its line items
            needs a WBS code that could not be created.
            """
            wbs_code_mapping = {}
            for wbs in rail.result('get_wbs_codes')['existing']:
                flat_code = wbs.get('flat_code')
                wbs_code_id = wbs.get('id')
                if flat_code and wbs_code_id:
                    wbs_code_mapping[flat_code] = wbs_code_id

            failed_codes = set()
            failed_reasons = {}
            
            for result in get_created_wbs_results():
                for flat_code, value in result.items():
                    if not flat_code:
                        continue
                    if isinstance(value, str):
                        failed_codes.add(flat_code)
                        failed_reasons[flat_code] = value
                    else:
                        wbs_code_mapping[flat_code] = value

            purchase_orders = dag_run.conf['purchase_order_batch']['purchase_orders']
            syncable = []
            skipped = []
            po_status = {}
            for po in purchase_orders:
                po_flat_codes = set()
                for item in po['line_items']:
                    flat_code = build_flat_code(
                        item['phase_code'], item['category_code'], item['cost_type'])
                    if flat_code:
                        po_flat_codes.add(flat_code)

                blocking = sorted(po_flat_codes & failed_codes)
                if blocking:
                    skipped.append({'po': po, 'failed_codes': blocking})
                else:
                    syncable.append(po)
                    po_status[f"CE_{po['number']}"] = po['status']
            
            vendors_needing_assignment = list({
                po['vendor_id']
                for po in syncable
                if po['should_assign_contractor_to_project'] and po['vendor_id']
            })
            return {
                'wbs_codes': wbs_code_mapping,
                'syncable': syncable,
                'skipped': skipped,
                'po_status': po_status,
                'failed_reasons': failed_reasons,
                'vendors_to_assign': vendors_needing_assignment
            }

        syncable_purchase_orders = rail.PythonOperator(
            task_id='syncable_purchase_orders',
            python_callable=build_syncable_purchase_orders
        )

        # Alert on POs skipped due to failed WBS code creation
        has_skipped_pos = rail.IfOperator(
            task_id='has_skipped_pos',
            test='{{ result("syncable_purchase_orders").skipped | length > 0 }}',
            yes_task='write_skipped_po_alert',
            no_task='fetch_procore_purchase_orders'
        )

        write_skipped_po_alert = rail.WriteLogOperator(
            task_id='write_skipped_po_alert',
            message='Purchase Order Skipped - WBS Code Creation Failed',
            severity='Error/Exception',
            properties=lambda item: item,
            items=lambda dag_run: [
                {
                    'code': entry['po'].get('number', ''),
                    'job_code': dag_run.conf['purchase_order_batch']['job_code'],
                    'company_id': dag_run.conf['company_id'],
                    'status': 'Skipped',
                    'reason': 'PO not synced; WBS codes could not be created: '
                        + ', '.join([
                            rail.result('syncable_purchase_orders')['failed_reasons'][code]
                            for code in entry['failed_codes']
                        ])
                } for entry in rail.result('syncable_purchase_orders')['skipped']
            ]
        )

        # Fetch existing POs with embedded line items for matching
        fetch_procore_purchase_orders = rail.ProcoreApiOperator(
            task_id='fetch_procore_purchase_orders',
            endpoint='/purchase_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'view': 'extended',
                'project_id': dag_run.conf['purchase_order_batch']['project_id'],
                **(
                    {
                        'filters[origin_id]': '[' + ','.join(list(map(
                            lambda po: 'CE_' + po['number'],
                            rail.result('syncable_purchase_orders')['syncable']
                        ))) + ']'
                    } if rail.result('syncable_purchase_orders')['syncable'] else {}
                )
            },
            data_handler=lambda response: list(map(lambda x: {
                'id': x['id'],
                'number': x['number'],
                'origin_id': x['origin_id'],
                'accounting_method': x['accounting_method'],
                'line_items': x['line_items']
            }, response)
            ) if response else []
        )

        has_vendors_to_assign = rail.IfOperator(
            task_id='has_vendors_to_assign',
            test=lambda: len(rail.result('syncable_purchase_orders')['vendors_to_assign']) > 0,
            yes_task='for_each_vendor_to_assign',
            no_task='sync_purchase_orders'
        )

        for_each_vendor_to_assign = rail.ForEachOperator(
            task_id='for_each_vendor_to_assign',
            items=lambda: rail.result('syncable_purchase_orders')['vendors_to_assign'],
            start_task='assign_vendor_to_project',
            end_task='end_assign_vendor'
        )

        assign_vendor_to_project = rail.ProcoreApiOperator(
            task_id='assign_vendor_to_project',
            endpoint=lambda dag_run: f"/projects/{dag_run.conf['purchase_order_batch']['project_id']}/vendors/{rail.result('for_each_vendor_to_assign')}/actions/add",
            method='POST'
        )

        end_assign_vendor = rail.EmptyOperator(
            task_id='end_assign_vendor'
        )

        # Build PO sync payload with origin_id matching
        def get_purchase_order_payload(dag_run):
            batch = dag_run.conf['purchase_order_batch']
            project_id = batch['project_id']
            purchase_orders = rail.result('syncable_purchase_orders')['syncable']

            existing_pos = rail.result('fetch_procore_purchase_orders') or []
            existing_by_origin_id = {
                po['origin_id']: po for po in existing_pos if po.get('origin_id')}

            updates = []
            for po in purchase_orders:
                origin_id = f"CE_{po['number']}"
                existing_po = existing_by_origin_id.get(origin_id)
                accounting_method = existing_po['accounting_method'] if existing_po else 'unit'

                updates.append({
                    'number': po['number'],
                    'vendor_id': po['vendor_id'],
                    'origin_id': origin_id,
                    'origin_data': 'Synced from Computerease',
                    'title': po['title'],
                    'description': po['description'],
                    'issued_on_date': convert_date(po['issued_on_date']),
                    'delivery_date': convert_date(po['delivery_date']),
                    'contract_date': convert_date(po['contract_date']),
                    'status': ProcorePurchaseOrderStatus.DRAFT,
                    'bill_to_address': po['bill_to_address'],
                    'ship_to_address': po['bill_to_address'],
                    'accounting_method': accounting_method,
                    'executed': True,
                    'private': True
                })

            return {
                'project_id': project_id,
                'updates': updates
            }

        # Sync POs (creates/updates) using origin_id matching
        sync_purchase_orders = rail.ProcoreApiOperator(
            task_id='sync_purchase_orders',
            endpoint='/purchase_order_contracts/sync',
            method='PATCH',
            data=get_purchase_order_payload,
            data_handler=lambda res: {
                'entities': list(map(lambda x: {
                    'id': x['id'],
                    'number': x['number'],
                    'origin_id': x['origin_id'],
                    'vendor_id': x['vendor']['id'] if x.get('vendor') else None
                }, res.get('entities', []))
                ) if res else [],
                'errors': res.get('errors', []) if res else []
            }
        )

        # Check if there are sync errors to log
        has_po_sync_errors = rail.IfOperator(
            task_id='has_po_sync_errors',
            test='{{ result("sync_purchase_orders").errors | length > 0 }}',
            yes_task='write_po_sync_errors',
            no_task='all_line_items_to_sync'
        )

        write_po_sync_errors = rail.WriteLogOperator(
            task_id='write_po_sync_errors',
            message='Purchase Order Sync Error',
            severity='Error/Exception',
            properties=lambda item: item,
            items=lambda dag_run: [
                {
                    'code': error.get('id', 'unknown'),
                    'job_code': dag_run.conf['purchase_order_batch']['job_code'],
                    'company_id': dag_run.conf['company_id'],
                    'status': 'Sync Error',
                    'reason': ', '.join([
                        f"{field}: {'; '.join(messages)}"
                        for field, messages in error.get('errors', {}).items()
                    ]) if error.get('errors') else 'Unknown error'
                } for error in rail.result('sync_purchase_orders')['errors']
            ]
        )

        # Map synced PO IDs to embedded line items
        def get_all_line_items_to_sync():
            """Returns list of POs with their line items for ForEach processing."""

            sync_result = rail.result('sync_purchase_orders')
            synced_entities = sync_result.get(
                'entities', []) if sync_result else []
            if not synced_entities:
                return []

            existing_pos = rail.result('fetch_procore_purchase_orders')

            # Mapping by origin_id (PO number) for reliable matching
            # Filter out POs with missing origin_id to prevent None keys
            existing_by_origin_id = {
                po['origin_id']: po for po in existing_pos if po.get('origin_id')
            }

            # Mapping by PO number as fallback
            # Filter out POs with missing number to prevent None keys
            existing_by_number = {
                po['number']: po for po in existing_pos if po.get('number')
            }

            # Build list of PO objects with their line items for ForEach processing
            po_list = []
            for synced_po in synced_entities:
                po_id = synced_po['id']
                po_number = synced_po['number']
                origin_id = synced_po['origin_id']

                # Try to find existing PO data by origin_id first, then by number
                existing_po = None
                if origin_id and origin_id in existing_by_origin_id:
                    existing_po = existing_by_origin_id[origin_id]
                elif po_number in existing_by_number:
                    existing_po = existing_by_number[po_number]

                # Extract line items from existing PO (empty array if new PO or no line items)
                existing_line_items = existing_po['line_items'] if existing_po else [
                ]

                po_list.append({
                    'po_id': po_id,
                    'po_number': po_number,
                    'origin_id': origin_id,
                    'existing_line_items': existing_line_items
                })

            return po_list

        # Map PO IDs to their line items
        all_line_items_to_sync = rail.PythonOperator(
            task_id='all_line_items_to_sync',
            python_callable=get_all_line_items_to_sync
        )

        def get_sov_payloads(dag_run):
            """Builds the full SOV child-DAG conf payload for each synced PO."""
            batch = dag_run.conf['purchase_order_batch']
            po_line_items = {}
            for po in batch['purchase_orders']:
                po_line_items[po['number']] = po['line_items']

            return list(map(lambda po: {
                'po_data': po,
                'ce_po_line_items': po_line_items.get(po['po_number']),
                'wbs_code_mapping': rail.result('syncable_purchase_orders')['wbs_codes'],
                'company_id': dag_run.conf['company_id'],
                'project_id': batch['project_id'],
                'job_code': batch['job_code']
            }, rail.result('all_line_items_to_sync')))

        # Trigger one SOV sync DAG per PO
        trigger_sov_sync_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_sov_sync_dags',
            items=get_sov_payloads,
            trigger_dag_id=config.sov_sync_dag_id,
            conf=lambda item: item,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_sov_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_sov_sync',
            dag_runs='{{ result("trigger_sov_sync_dags") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_approve_purchase_order_payload(dag_run):
            """Returns payload to approve synced POs that have a vendor assigned."""
            project_id = dag_run.conf['purchase_order_batch']['project_id']
            po_status = rail.result('syncable_purchase_orders')['po_status']
            synced_purchase_orders = rail.result('sync_purchase_orders')['entities']

            updates = list(map(lambda po: {
                'origin_id': po['origin_id'],
                'status': po_status.get(po['origin_id'], ProcorePurchaseOrderStatus.DRAFT) \
                    if po['vendor_id'] else ProcorePurchaseOrderStatus.DRAFT
            }, synced_purchase_orders))

            return {
                'project_id': project_id,
                'updates': updates
            }
        approve_synced_pos = rail.ProcoreApiOperator(
            task_id='approve_synced_pos',
            endpoint='/purchase_order_contracts/sync',
            method='PATCH',
            data=get_approve_purchase_order_payload
        )

        def get_error_details(dag_run):
            company_id = dag_run.conf.get('company_id', 'unknown')
            batch = dag_run.conf.get('purchase_order_batch')
            job_code = batch.get('job_code', 'unknown')
            po_numbers = ','.join(
                list(map(lambda x: x.get('number', ''), batch['purchase_orders'])))

            err = rail.render_template('{{ get_error_message() }}')
            if type(err) == str:
                status = 'Error'
                reason = err
            else:
                status = err['response']['status_code'] \
                    if err.get('response') else 'Error'
                reason = err['response']['json']['error']['reason'] \
                    if err.get('response') else err

            return {
                'code': po_numbers,
                'job_code': job_code,
                'company_id': company_id,
                'status': status,
                'reason': reason
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error >> log_to_sumo
        batch_task >> get_wbs_codes >> has_missing_codes

        has_missing_codes >> rail.Label('No') >> syncable_purchase_orders
        has_missing_codes >> rail.Label(
            'Yes') >> trigger_wbs_code_creation >> wait_for_wbs_code_creation >> gather_wbs_creation_results
        gather_wbs_creation_results >> syncable_purchase_orders >> has_skipped_pos

        has_skipped_pos >> rail.Label('Yes') >> write_skipped_po_alert >> fetch_procore_purchase_orders
        has_skipped_pos >> rail.Label('No') >> fetch_procore_purchase_orders

        fetch_procore_purchase_orders >> has_vendors_to_assign

        has_vendors_to_assign >> rail.Label('No') >> sync_purchase_orders
        has_vendors_to_assign >> rail.Label('Yes') >> for_each_vendor_to_assign

        for_each_vendor_to_assign >> assign_vendor_to_project >> end_assign_vendor
        for_each_vendor_to_assign >> end_assign_vendor >> sync_purchase_orders

        sync_purchase_orders >> has_po_sync_errors
        has_po_sync_errors >> rail.Label(
            'Yes') >> write_po_sync_errors >> all_line_items_to_sync
        has_po_sync_errors >> rail.Label('No') >> all_line_items_to_sync

        # Line item sync flow - trigger SOV sync DAG per PO
        all_line_items_to_sync >> trigger_sov_sync_dags >> wait_for_sov_sync >> approve_synced_pos >> catch_error

        return dag


rail.for_each_instance(create_dag_instance)
