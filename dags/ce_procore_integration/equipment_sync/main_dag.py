import rail
from datetime import timedelta
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from rail.operators.procore.version_mapper import ProcoreEquipmentVersion
from ce_procore_integration.equipment_sync.utils.constants import ProcoreEquipmentStatus, Operation
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Computerease to Procore Equipment Sync',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        schedule_interval=timedelta(
            minutes=config.equipment_sync_interval_minutes),
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        procore_company_id_template = "{{ conn." + \
            config.procore_conn_id + ".extra_dejson.company_id }}"

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='last_sync_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        last_sync_time = rail.PythonOperator(
            task_id='last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.equipment_last_sync_time_var,
                date_format=config.ce_time_format,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False
            )
        )

        fetch_computerease_equipments = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_equipments',
            endpoint='/catalog/equipment',
            request_method='GET',
            query_params={
                'fields': config.computerease_required_fields,
                'gt~updated_at': "{{ result('last_sync_time')['last_synctime'] }}",
            },
            data_handler=lambda response: response.get(
                'data', []) if response else []
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.equipment_last_sync_time_var,
                value_to_set=rail.result('last_sync_time')['current_time']
            )
        )

        has_equipments_to_sync = rail.IfOperator(
            task_id='has_equipments_to_sync',
            test='{{ result("fetch_computerease_equipments") | length > 0 }}',
            yes_task='fetch_procore_equipments',
            no_task='log_to_sumo'
        )

        def create_batches(create_items, update_items):
            batches = []
            # Individual items for CREATE (no bulk API available yet)
            for item in create_items:
                batches.append({
                    'operation': Operation.CREATE,
                    'equipments': item
                })

            # Batch items for UPDATE
            for i in range(0, len(update_items), config.batch_size):
                batch = update_items[i:i + config.batch_size]
                batches.append({
                    'operation': Operation.UPDATE,
                    'equipments': batch
                })
            return batches

        def get_equipments_to_sync(procore_equipments):
            computerease_equipments = rail.result(
                'fetch_computerease_equipments')
            procore_equipments_lookup = {}
            for item in procore_equipments:
                key = item['identification_number']
                procore_equipments_lookup[key] = {
                    'id': item['id'],
                    'name': item['name'],
                    'status': item['status']['name']
                }

            create_items = []
            update_items = []

            for equipment in computerease_equipments:
                ce_code = equipment['code']
                ce_status = equipment['active']
                ce_name = equipment['description']

                base_item = {
                    'ce_code': ce_code,
                    'ce_name': ce_name,
                    'ce_status': ce_status
                }

                if ce_code in procore_equipments_lookup:
                    procore_item = procore_equipments_lookup[ce_code]
                    is_procore_active = procore_item['status'] == ProcoreEquipmentStatus.ACTIVE

                    # update equipment if name or status is modified
                    if ce_name != procore_item['name'] or ce_status != is_procore_active:
                        base_item['procore_id'] = procore_item['id']
                        update_items.append(base_item)
                else:
                    create_items.append(base_item)

            return create_batches(create_items, update_items)

        fetch_procore_equipments = rail.ProcoreApiOperator(
            task_id='fetch_procore_equipments',
            endpoint=lambda: f'companies/{rail.render_template(procore_company_id_template)}/equipment_register',
            method='GET',
            version=ProcoreEquipmentVersion.EQUIPMENT_REGISTER,
            data_handler=lambda res: {
                'raw_response': res,
                'equipments': get_equipments_to_sync(res)
            }
        )

        fetch_procore_equipments_statuses = rail.ProcoreApiOperator(
            task_id='fetch_procore_equipments_statuses',
            endpoint=lambda: f'companies/{rail.render_template(procore_company_id_template)}/equipment_register/statuses',
            method='GET',
            version=ProcoreEquipmentVersion.EQUIPMENT_ITEMS
        )

        fetch_procore_equipment_types = rail.ProcoreApiOperator(
            task_id='fetch_procore_equipment_types',
            endpoint=lambda: f'companies/{rail.render_template(procore_company_id_template)}/equipment_register_types',
            method='GET',
            version=ProcoreEquipmentVersion.EQUIPMENT_ITEMS,
            data_handler=lambda res: list(filter(lambda item: item['name'] == config.default_equipment_type and \
                                                 item['category']['name'] == config.default_equipment_category, res or []))
        )

        does_category_and_type_exists = rail.IfOperator(
            task_id='does_category_and_type_exists',
            test='{{ result("fetch_procore_equipment_types") | length > 0 }}',
            yes_task='trigger_equipment_sync',
            no_task='create_procore_equipment_category'
        )

        create_procore_equipment_category = rail.ProcoreApiOperator(
            task_id='create_procore_equipment_category',
            endpoint=lambda: f'companies/{rail.render_template(procore_company_id_template)}/equipment_register_categories',
            method='POST',
            version=ProcoreEquipmentVersion.EQUIPMENT_ITEMS,
            data={'name': config.default_equipment_category, 'is_active': True},
            data_handler=lambda res: res.get('data', {}).get('id', '')
        )

        create_procore_equipment_type = rail.ProcoreApiOperator(
            task_id='create_procore_equipment_type',
            endpoint=lambda: f'companies/{rail.render_template(procore_company_id_template)}/equipment_register_types',
            method='POST',
            version=ProcoreEquipmentVersion.EQUIPMENT_ITEMS,
            data=lambda: {'name': config.default_equipment_type, 'category_id': rail.result(
                'create_procore_equipment_category'), 'is_active': True},
            data_handler=lambda res: res.get('data', {}).get('id')
        )

        trigger_equipment_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_equipment_sync',
            items=lambda: rail.result('fetch_procore_equipments')['equipments'],
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'batch': item,
                'statuses': rail.result('fetch_procore_equipments_statuses'),
                'company_id': rail.render_template(procore_company_id_template),
                'type_id': rail.result('fetch_procore_equipment_types')[0]['id'] if rail.result('fetch_procore_equipment_types') else rail.result('create_procore_equipment_type'),
                'category_id': rail.result('fetch_procore_equipment_types')[0]['category']['id'] if rail.result('fetch_procore_equipment_types') else rail.result('create_procore_equipment_category')
            }
        )

        wait_for_equipment_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_equipment_sync',
            dag_runs='{{ result("trigger_equipment_sync") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test='{{ result("search_logs", "length") > 0 }}',
            yes_task='write_logs_into_csv',
            no_task='log_to_sumo'
        )

        write_logs_into_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_into_csv',
            source='{{ result("search_logs") }}',
            header=['Operation', 'Equipment Code',
                    'Equipment Name', 'Status', 'Reason', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('operation','') }}",
                "{{ item.properties | attr_or_default('code','') }}",
                "{{ item.properties | attr_or_default('name','') }}",
                "{{ item.properties | attr_or_default('status','') }}",
                "{{ item.properties | attr_or_default('reason','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_EquipmentSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_alert = rail.EmailOperator(
            task_id='send_email_alert',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: Equipment Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/equipment_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> last_sync_time >> fetch_computerease_equipments >> set_last_sync_time >> has_equipments_to_sync

        has_equipments_to_sync >> rail.Label('No') >> log_to_sumo
        has_equipments_to_sync >> rail.Label(
            'Yes') >> fetch_procore_equipments >> fetch_procore_equipments_statuses

        fetch_procore_equipments_statuses >> fetch_procore_equipment_types >> does_category_and_type_exists

        does_category_and_type_exists >> rail.Label(
            'Yes') >> trigger_equipment_sync
        does_category_and_type_exists >> rail.Label(
            'No') >> create_procore_equipment_category >> create_procore_equipment_type >> trigger_equipment_sync

        trigger_equipment_sync >> wait_for_equipment_sync >> search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_download_link >> send_email_alert >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
