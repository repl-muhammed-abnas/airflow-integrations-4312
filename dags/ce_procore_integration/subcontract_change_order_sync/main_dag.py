import rail
from datetime import timedelta, datetime
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Computerease to Procore Subcontract Change Order Sync',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        schedule_interval=timedelta(
            minutes=config.subcontract_change_order_sync_interval_minutes),
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        procore_company_id_template = "{{ conn." + \
            config.procore_conn_id + ".extra_dejson.company_id }}"

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.subcontract_change_order_last_sync_time_var,
                date_format=config.ce_time_format,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False
            ),
        )

        fetch_computerease_subcontracts = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_subcontracts',
            endpoint='/catalog/subcontract',
            request_method='GET',
            page_limit = 1000,
            data_handler=lambda resp: resp['data'] if 'data' in resp else None
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.subcontract_change_order_last_sync_time_var,
                value_to_set=rail.result('get_last_sync_time')['current_time']
            )
        )

        def get_computerease_subcontract_change_orders_data():
            subcontracts = rail.result('fetch_computerease_subcontracts')
            change_orders = []
            last_sync_time_str = rail.result('get_last_sync_time')[
                'last_synctime']
            last_sync_time = datetime.strptime(
                last_sync_time_str, config.ce_time_format)

            for subcontract in subcontracts:
                updated_at_str = subcontract.get('updated_at', '')
                rfc_code = subcontract.get('rfc_code', '')
                updated_at = datetime.strptime(
                    updated_at_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                approval_status = subcontract.get('approval_status', '')
                if (updated_at >= last_sync_time and rfc_code != '' and ((config.sync_only_approved_change_orders.lower() == 'yes' and approval_status == 'approved') or config.sync_only_approved_change_orders.lower() != 'yes')):
                    change_orders.append(subcontract)
            return change_orders

        fetch_subcontract_change_orders = rail.PythonOperator(
            task_id='fetch_subcontract_change_orders',
            python_callable=get_computerease_subcontract_change_orders_data
        )

        has_change_orders_to_sync = rail.IfOperator(
            task_id='has_change_orders_to_sync',
            test='{{ result("fetch_subcontract_change_orders") | length > 0 }}',
            yes_task='fetch_computerease_cost_types',
            no_task='search_logs'
        )

        fetch_computerease_cost_types = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_cost_types',
            endpoint='/catalog/cost-type',
            request_method='GET',
            data_handler=lambda resp: resp['data'] if 'data' in resp else None
        )

        fetch_procore_change_order_change_reasons = rail.ProcoreApiOperator(
            task_id='fetch_procore_change_order_change_reasons',
            endpoint='/change_order_change_reasons',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            }
        )

        fetch_procore_projects = rail.ProcoreApiOperator(
            task_id='fetch_procore_projects',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            },
            data_handler=lambda projects: {
                x['origin_id']: x['id'] for x in projects if x['active']} if projects else {}
        )

        def get_project_id(item):
            projects = rail.result('fetch_procore_projects')
            origin_id = f"CE_{item['job_code']}"
            return projects[origin_id] if origin_id in projects else None

        trigger_subcontract_change_order_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_subcontract_change_order_sync',
            items=lambda: rail.result('fetch_subcontract_change_orders'),
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'change_order': item,
                'company_id': rail.render_template(procore_company_id_template),
                'change_order_change_reasons': rail.result('fetch_procore_change_order_change_reasons'),
                'project_id': get_project_id(item),
                'cost_types': rail.result('fetch_computerease_cost_types')
            }
        )

        wait_for_subcontract_change_order_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_subcontract_change_order_sync',
            dag_runs='{{ result("trigger_subcontract_change_order_sync") }}',
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
            header=['RFC Code', 'Project Id', 'Commitment Contract Id',
                    'CCO_payload', 'line_item_payload', 'Reason', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('rfc_code','') }}",
                "{{ item.properties | attr_or_default('project_id','') }}",
                "{{ item.properties | attr_or_default('commitment_contract_id','') }}",
                "{{ item.properties | attr_or_default('CCO_payload','') }}",
                "{{ item.properties | attr_or_default('line_item_payload','') }}",
                "{{ item.properties | attr_or_default('reason','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_SubcontractChangeOrderSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_alert = rail.EmailOperator(
            task_id='send_email_alert',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: Subcontract Change Order Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/subcontract_change_order_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> get_last_sync_time >> fetch_computerease_subcontracts >> set_last_sync_time >> fetch_subcontract_change_orders >> has_change_orders_to_sync

        has_change_orders_to_sync >> rail.Label('Yes') >> fetch_computerease_cost_types >> fetch_procore_change_order_change_reasons >> fetch_procore_projects \
            >> trigger_subcontract_change_order_sync >> wait_for_subcontract_change_order_sync >> search_logs >> if_logs_present
        has_change_orders_to_sync >> rail.Label(
            'No') >> search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_download_link >> send_email_alert >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
