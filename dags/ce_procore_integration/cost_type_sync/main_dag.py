from datetime import timedelta
import rail
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Computerease to Procore Cost Type Sync',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        schedule_interval=timedelta(
            minutes=config.cost_type_sync_interval_minutes),
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
            start_task='fetch_computerease_cost_types',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        fetch_computerease_cost_types = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_cost_types',
            endpoint='/catalog/cost-type',
            request_method='GET',
            data_handler=lambda cost_types: [
                {
                    'code': item.get('reference'),
                    'name': item.get('description')
                } for item in cost_types['data']
            ] if cost_types.get('data', None) else None
        )

        fetch_cost_type_segment_id = rail.ProcoreApiOperator(
            task_id='fetch_cost_type_segment_id',
            endpoint=f'/companies/{procore_company_id_template}/work_breakdown_structure/segments',
            method='GET',
            data_handler=lambda wbs_segments: next(
                (
                    segment['id'] for segment in wbs_segments
                    if segment['name'] == config.cost_type_name
                    and segment['type'] == config.cost_type_type
                ),
                None
            )
        )

        fetch_existing_cost_types = rail.ProcoreApiOperator(
            task_id='fetch_existing_cost_types',
            endpoint=lambda: f'/companies/{rail.render_template(procore_company_id_template)}/work_breakdown_structure/segments/{rail.render_template(rail.result("fetch_cost_type_segment_id"))}/segment_items',
            method='GET'
        )

        def get_cost_types_to_sync():
            computerease_cost_types = rail.result(
                'fetch_computerease_cost_types')
            procore_cost_types = rail.result('fetch_existing_cost_types')
            procore_cost_type_codes = {
                item['code']: item for item in procore_cost_types}

            cost_types = []
            for computerease_cost_type in computerease_cost_types:
                ce_cost_type_code = computerease_cost_type['code']
                ce_cost_type_name = computerease_cost_type['name']

                if ce_cost_type_code not in procore_cost_type_codes:
                    cost_types.append({
                        'name': ce_cost_type_name,
                        'code': ce_cost_type_code
                    })
                else:
                    procore_item = procore_cost_type_codes[ce_cost_type_code]
                    if ce_cost_type_name != procore_item['name']:
                        cost_types.append({
                            'procore_id': procore_item['id'],
                            'name': ce_cost_type_name,
                            'code': procore_item['code']
                        })
            return cost_types

        trigger_cost_type_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_cost_type_sync',
            items=get_cost_types_to_sync,
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'cost_type': item,
                'segment_id': rail.result('fetch_cost_type_segment_id'),
                'company_id': rail.render_template(procore_company_id_template)
            }
        )

        wait_for_cost_type_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_cost_type_sync',
            dag_runs='{{ result("trigger_cost_type_sync") }}',
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
            header=['Cost Type Code', 'Cost Type Name',
                    'Status', 'Reason', 'ECID'],
            row=[
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
            output_file_name='ComputereaseProcore_CostTypeSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_alert = rail.EmailOperator(
            task_id='send_email_alert',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: Cost Type Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/cost_type_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> fetch_computerease_cost_types >> fetch_cost_type_segment_id >> fetch_existing_cost_types
        fetch_existing_cost_types >> trigger_cost_type_sync >> wait_for_cost_type_sync >> search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_download_link >> send_email_alert >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
