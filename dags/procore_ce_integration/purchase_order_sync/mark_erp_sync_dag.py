from datetime import timedelta
import rail


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'procore_computerease_purchase_order_mark_erp_sync_{config.instance}',
        description='Procore Purchase Order Sync - Mark ERP Sync (set origin_id after CE import acceptance)',
        integration_type='generic',
        company_key=config.instance,
        schedule_interval=timedelta(seconds=config.origin_id_update_schedule_seconds),
        max_active_runs=config.max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id
        }
    ) as dag:

        # db is located by integration+customer; collection_name is the table.
        collection_integration = config.s3_collection['integration']
        collection_name = config.origin_id_update_table['name']

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_update_enabled',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        if_update_enabled = rail.IfOperator(
            task_id='if_update_enabled',
            test=lambda: config.defer_origin_id_until_accepted,
            yes_task='query_pending',
            no_task='log_to_sumo'
        )

        query_pending = rail.S3QueryCollectionOperator(
            task_id='query_pending',
            query=f'SELECT {", ".join(config.origin_id_update_table["columns"])} FROM {collection_name}',  # pylint: disable=line-too-long
            integration=collection_integration,
            customer=config.instance,
            mode='multi-row'
        )

        if_pending_present = rail.IfOperator(
            task_id='if_pending_present',
            test=lambda: len(rail.result('query_pending')) > 0,
            yes_task='fetch_imports',
            no_task='log_to_sumo'
        )

        def group_imports_by_status(response):
            accepted, rejected = [], []
            for item in response.get('data', []):
                uuid = item.get('uuid')
                if not uuid:
                    continue
                if item.get('status') == 'accepted':
                    accepted.append(uuid)
                elif item.get('status') == 'rejected':
                    rejected.append(uuid)
            return {'accepted': accepted, 'rejected': rejected}

        # Single call; partition accepted/rejected by each import's status.
        fetch_imports = rail.ComputereaseAPIOperator(
            task_id='fetch_imports',
            endpoint='/import/',
            request_method='GET',
            query_params={
                'import_type': 'Purchase Orders',
                'sort': '-updated_at'
            },
            data_handler=group_imports_by_status
        )

        def resolve_pending():
            pending = rail.result('query_pending') or []
            imports = rail.result('fetch_imports') or {}
            accepted = set(imports.get('accepted', []))
            rejected = set(imports.get('rejected', []))
            updates, delete_params = [], []
            for row in pending:
                import_uuid = row.get('import_uuid')
                purchase_order_id = row.get('purchase_order_id')
                if import_uuid in accepted:
                    updates.append({
                        'purchase_order_id': purchase_order_id,
                        'project_id': row.get('project_id'),
                        'origin_id': row.get('origin_id')
                    })
                    # Scope delete to this exact import so a newer re-queued row (same PO,
                    # different import_uuid) upserted after query_pending survives.
                    delete_params.append([purchase_order_id, import_uuid])
                elif import_uuid in rejected:
                    print(f"Origin ID not set: ComputerEase import '{import_uuid}' for "
                          f"purchase order '{purchase_order_id}' was rejected; link abandoned.")
                    delete_params.append([purchase_order_id, import_uuid])
            return {
                'updates': updates,
                'delete_params': delete_params,
                'updates_count': len(updates),
                'delete_count': len(delete_params)
            }

        resolve_pending_task = rail.PythonOperator(
            task_id='resolve_pending',
            python_callable=resolve_pending
        )

        if_updates_present = rail.IfOperator(
            task_id='if_updates_present',
            test=lambda: rail.result('resolve_pending')['updates_count'] > 0,
            yes_task='for_each_set_origin_id',
            no_task='if_deletes_present'
        )

        # Purchase order sync writes origin_id per PO, so PATCH each accepted row.
        for_each_set_origin_id = rail.ForEachOperator(
            task_id='for_each_set_origin_id',
            items=lambda: rail.result('resolve_pending')['updates'],
            start_task='patch_origin_id',
            end_task='end_patch_origin_id'
        )

        patch_origin_id = rail.ProcoreApiOperator(
            task_id='patch_origin_id',
            endpoint="/purchase_order_contracts/{{ result('for_each_set_origin_id').purchase_order_id }}",
            method='PATCH',
            query_params={
                'project_id': "{{ result('for_each_set_origin_id').project_id }}"
            },
            data=lambda: {
                'purchase_order_contract': {
                    'origin_id': rail.result('for_each_set_origin_id')['origin_id']
                }
            }
        )

        end_patch_origin_id = rail.EmptyOperator(
            task_id='end_patch_origin_id'
        )

        if_deletes_present = rail.IfOperator(
            task_id='if_deletes_present',
            test=lambda: rail.result('resolve_pending')['delete_count'] > 0,
            yes_task='build_delete_params',
            no_task='log_to_sumo'
        )

        build_delete_params_task = rail.PythonOperator(
            task_id='build_delete_params',
            python_callable=lambda: rail.result('resolve_pending')['delete_params']
        )

        delete_resolved_rows = rail.S3UpdateCollectionOperator(
            task_id='delete_resolved_rows',
            integration=collection_integration,
            customer=config.instance,
            collection_name=collection_name,
            query=f'DELETE FROM {collection_name} WHERE purchase_order_id = ? AND import_uuid = ?',
            query_params_list=build_delete_params_task.output,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> if_update_enabled
        batch_task >> log_to_sumo

        if_update_enabled >> rail.Label('Yes') >> query_pending >> if_pending_present
        if_update_enabled >> rail.Label('No') >> log_to_sumo

        if_pending_present >> rail.Label(
            'Yes') >> fetch_imports >> resolve_pending_task >> if_updates_present
        if_pending_present >> rail.Label('No') >> log_to_sumo

        if_updates_present >> rail.Label('Yes') >> for_each_set_origin_id
        for_each_set_origin_id >> patch_origin_id >> end_patch_origin_id
        for_each_set_origin_id >> end_patch_origin_id >> if_deletes_present
        if_updates_present >> rail.Label('No') >> if_deletes_present

        if_deletes_present >> rail.Label(
            'Yes') >> build_delete_params_task >> delete_resolved_rows >> log_to_sumo
        if_deletes_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
