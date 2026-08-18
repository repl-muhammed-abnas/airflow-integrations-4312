import rail
from datetime import timedelta


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.sov_sync_dag_id,
        description='Sync Schedule of Values (Line Items) per PO',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.sov_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='line_items_batch',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Build line item sync payload using accounting codes + sequential numbering for duplicates
        # origin_id: {po_number}|{job_code}|{phase}|{category}|{cost_type}[_1, _2...]
        # Tracks orphaned Procore items for deletion
        def get_line_items_batch(dag_run):
            # Get data from conf
            wbs_code_mapping = dag_run.conf['wbs_code_mapping']
            po_data = dag_run.conf['po_data']
            ce_line_items = dag_run.conf['ce_po_line_items']
            job_code = dag_run.conf['job_code']

            po_id = po_data['po_id']
            po_number = po_data['po_number']
            existing_line_items = po_data['existing_line_items']

            # This also deletes the line items procore if deleted from computerease
            if not ce_line_items:
                # All existing items with origin_id become orphaned if no CE items
                orphaned_ids = [item['id'] for item in existing_line_items if item.get(
                    'id') and item.get('origin_id')]
                return {'updates': [], 'orphaned_ids': orphaned_ids, 'po_id': po_id, 'po_number': po_number}

            # Sort CE line items for stable sequential numbering across syncs
            # Prevents origin_id changes if CE reorders items
            ce_line_items_sorted = sorted(ce_line_items, key=lambda x: (
                x.get('description', ''),
                x.get('phase_code', ''),
                x.get('category_code', ''),
                x.get('cost_type', '')
            ))

            # Step 1: Build origin_ids from CE with duplicate handling
            origin_id_counts = {}  # Track counts for each base origin_id
            ce_items_with_origin = []  # [(ce_item, final_origin_id)]

            for ce_item in ce_line_items_sorted:
                # Normalize accounting codes to uppercase for consistent matching
                phase = ce_item.get('phase_code', '')
                category = ce_item.get('category_code', '')
                cost_type = ce_item.get('cost_type', '')
                equip_num = ce_item.get('equipment_number', '')
                equip_code = ce_item.get('equipment_code', '')
                equipment = f'{equip_num}|{equip_code}' if equip_num and equip_code else equip_num or equip_code or ''

                base_origin_id = f'{po_number}|{job_code}|{phase}|{category}|{cost_type}|{equipment}'

                # Handle duplicates with sequential numbering
                if base_origin_id in origin_id_counts:
                    # Duplicate found - append sequence number
                    origin_id_counts[base_origin_id] += 1
                    final_origin_id = f'{base_origin_id}_{origin_id_counts[base_origin_id]}'
                else:
                    # First occurrence
                    origin_id_counts[base_origin_id] = 0
                    final_origin_id = base_origin_id

                ce_items_with_origin.append((ce_item, final_origin_id))

            # Step 2: Create mapping of existing Procore items by origin_id
            existing_by_origin_id = {}  # {origin_id: item}
            existing_ids_set = set()  # Track all existing item IDs

            for existing_item in existing_line_items:
                item_id = existing_item.get('id')
                origin_id = existing_item.get('origin_id', '')

                if item_id:
                    existing_ids_set.add(item_id)

                if origin_id:
                    existing_by_origin_id[origin_id] = existing_item

            # Helper function: Build line item update object

            def build_line_item_update_payload(ce_item, origin_id, procore_item_id=None):
                """Build update payload for line item sync with WBS code injection."""

                # Match and inject wbs_code_id from WBS management
                phase = ce_item.get('phase_code', '')
                category = ce_item.get('category_code', '')
                cost_type = ce_item.get('cost_type', '')
                if phase and category:
                    flat_code = f"{phase}-{category}.{cost_type}"
                else:
                    flat_code = f'{phase or category}.{cost_type}'

                equip_num = ce_item.get('equipment_number', '')
                equip_code = ce_item.get('equipment_code', '')
                origin_data = ''
                if equip_num and equip_code:
                    origin_data = '|'.join([equip_code, equip_num])
                elif equip_num or equip_code:
                    origin_data = equip_num or equip_code

                update_payload = {
                    'description': ce_item.get('description', ''),
                    'quantity': str(ce_item.get('quantity', 0)),
                    'unit_cost': str(ce_item.get('unit_cost', 0)),
                    'amount': str(ce_item.get('amount', 0)),
                    'uom': 'ea',
                    'extended_type': 'manual',
                    'origin_data': origin_data,
                    'origin_id': origin_id,
                    'wbs_code_id': wbs_code_mapping.get(flat_code, None)
                }
                # Include Procore item ID for updates (Phase 2: reusing orphans)
                if procore_item_id:
                    update_payload['id'] = procore_item_id

                return update_payload

            # Step 3: Three-phase line item matching
            all_updates = []
            matched_existing_ids = set()  # Track which existing Procore items were matched
            unmatched_ce_items = []  # Track CE items without exact origin_id match

            # PHASE 1: Exact origin_id matching
            for ce_item, origin_id in ce_items_with_origin:
                existing_item = existing_by_origin_id.get(origin_id)

                if existing_item:
                    # Exact match found - will update
                    matched_existing_ids.add(existing_item.get('id'))
                    update_payload = build_line_item_update_payload(
                        ce_item, origin_id)
                    all_updates.append(update_payload)
                else:
                    # No exact match - save for Phase 2
                    unmatched_ce_items.append((ce_item, origin_id))

            # Step 4: Identify initial orphans (items with origin_id not matched in Phase 1)
            orphaned_items = []
            for existing_item in existing_line_items:
                item_id = existing_item.get('id')
                origin_id = existing_item.get('origin_id', '')

                # Only items with origin_id can be orphaned (preserve manual entries)
                if item_id and item_id not in matched_existing_ids and origin_id:
                    orphaned_items.append(existing_item)

            # PHASE 2: Greedy orphan reuse (minimize deletes by reusing ANY orphan)
            still_unmatched_ce_items = []

            for ce_item, new_origin_id in unmatched_ce_items:
                if orphaned_items:
                    # Reuse first available orphan regardless of attributes
                    orphan = orphaned_items.pop(0)
                    orphan_id = orphan.get('id')
                    matched_existing_ids.add(orphan_id)
                    # Pass orphan's ID to update existing item (not create new)
                    update_payload = build_line_item_update_payload(
                        ce_item, new_origin_id, procore_item_id=orphan_id)
                    all_updates.append(update_payload)
                else:
                    # No more orphans available - will create in Phase 3
                    still_unmatched_ce_items.append((ce_item, new_origin_id))

            # PHASE 3: Create new items for remaining unmatched
            for ce_item, origin_id in still_unmatched_ce_items:
                update_payload = build_line_item_update_payload(
                    ce_item, origin_id)
                all_updates.append(update_payload)

            # Step 5: Remaining orphans to delete
            orphaned_ids = [item.get('id') for item in orphaned_items]

            # Return payload with PO context and orphaned items
            return {
                'po_id': po_id,
                'po_number': po_number,
                'updates': all_updates,
                'orphaned_ids': orphaned_ids
            }

        # Prepare line item sync payloads
        line_items_batch = rail.PythonOperator(
            task_id='line_items_batch',
            python_callable=get_line_items_batch
        )

        # Skip sync if no line items exist
        check_has_line_items = rail.IfOperator(
            task_id='check_has_line_items',
            test='{{ result("line_items_batch").updates | length > 0 }}',
            yes_task='sync_po_line_items',
            no_task='check_has_orphans'
        )

        # Bulk sync line items (creates/updates by origin_id)
        sync_po_line_items = rail.ProcoreApiOperator(
            task_id='sync_po_line_items',
            endpoint=lambda: f"/purchase_order_contracts/{rail.result('line_items_batch')['po_id']}/line_items/sync",
            method='PATCH',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data=lambda: {
                'updates': rail.result('line_items_batch')['updates']
            },
            data_handler=lambda res: {
                'po_number': rail.result('line_items_batch')['po_number'],
                'po_id': rail.result('line_items_batch')['po_id'],
                'synced_count': len(res.get('entities', [])) if res else 0,
                'sync_succeeded': True if res and res.get('entities') else False,
                'message': f"Synced {len(res.get('entities', []))} line items for PO {rail.result('line_items_batch')['po_number']}" if res else f"No line items synced for PO {rail.result('line_items_batch')['po_number']}"
            }
        )

        # Check if orphaned items need deletion (only if sync succeeded)

        def should_delete_orphans():
            """Check if we should delete orphans - only if sync succeeded or was skipped"""
            prep_result = rail.result('line_items_batch')
            orphaned_ids = prep_result.get('orphaned_ids', [])

            if not orphaned_ids:
                return False

            # Check if sync was performed
            sync_result = rail.result('sync_po_line_items')

            # If sync was skipped (sync_result is None or empty), safe to delete orphans
            if not sync_result:
                return True

            # If sync ran, verify it succeeded completely
            sync_succeeded = sync_result.get('sync_succeeded', False)
            synced_count = sync_result.get('synced_count', 0)
            expected_count = len(prep_result.get('updates', []))

            # Only delete orphans if sync fully succeeded
            return sync_succeeded and synced_count == expected_count

        check_has_orphans = rail.IfOperator(
            task_id='check_has_orphans',
            test=should_delete_orphans,
            yes_task='delete_line_items_loop',
            no_task='catch_error'
        )

        delete_line_items_loop = rail.ForEachOperator(
            task_id='delete_line_items_loop',
            items=lambda: rail.result(
                'line_items_batch').get('orphaned_ids', []),
            start_task='delete_line_item',
            end_task='delete_line_items_loop_end'
        )

        delete_line_item = rail.ProcoreApiOperator(
            task_id='delete_line_item',
            endpoint=lambda: f"/purchase_order_contracts/{rail.result('line_items_batch')['po_id']}/line_items/{rail.result('delete_line_items_loop')}",
            method='DELETE',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            }
        )

        delete_line_items_loop_end = rail.EmptyOperator(
            task_id='delete_line_items_loop_end'
        )

        def get_error_details(dag_run):
            company_id = dag_run.conf.get('company_id', 'unknown')
            job_code = dag_run.conf.get('job_code', 'unknown')
            po_data = dag_run.conf.get('po_data', {})
            po_number = po_data.get('po_number', 'unknown')

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
                'code': f"SOV Sync - PO: {po_number}",
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
        batch_task >> line_items_batch >> check_has_line_items

        check_has_line_items >> rail.Label(
            'Yes') >> sync_po_line_items >> check_has_orphans
        check_has_line_items >> rail.Label('No') >> check_has_orphans

        check_has_orphans >> rail.Label('Yes') >> delete_line_items_loop
        delete_line_items_loop >> delete_line_item >> delete_line_items_loop_end >> catch_error
        check_has_orphans >> rail.Label('No') >> catch_error

        return dag


rail.for_each_instance(create_dag_instance)
