from datetime import timedelta, datetime
import rail
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.subcontract_main_dag_id,
        description='Computerease to Procore Subcontract Sync MAIN DAG',
        integration_type='generic',
        company_key=config.instance,
        schedule_interval=timedelta(
            minutes=config.subcontract_sync_interval_minutes),
        max_active_runs=config.max_active_runs,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id,
        }
    ) as dag:

        procore_company_id_template = "{{conn." + \
            config.procore_conn_id + ".extra_dejson.company_id}}"

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.subcontract_last_sync_time_var,
                date_format=config.ce_time_format,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False
            )
        )

        def filter_out_subc_change_orders(subcontracts):
            return [
                subcontract for subcontract in subcontracts
                # Regular Subcontracts have it blank meanwhile Subc Change Orders have some value for it
                if not subcontract.get('rfc_code', False)
            ]

        def filter_subcontracts_by_timestamp_and_status(response_data):
            """
            Note: CE subcontract endpoint doesn't support server-side filtering
            (gt~updated_at not available). Once they start supporting it,
            we can remove this function and directly pass the filter in query params.

            For initial sync (when last_sync_time equals initial_sync_time), 
            only process subcontracts with allowed approval statuses.
            """
            subcontracts = filter_out_subc_change_orders(response_data)

            last_sync_time_str = rail.result('get_last_sync_time')[
                'last_synctime']
            last_sync_time = datetime.strptime(
                last_sync_time_str, config.ce_time_format)
            initial_sync_time = datetime.strptime(
                config.initial_sync_time, config.ce_time_format)

            # Check if this is the initial sync
            is_initial_sync = last_sync_time == initial_sync_time

            filtered_subcontracts = []
            for subcontract in subcontracts:
                subcontract_updated_at_str = subcontract.get('updated_at', '')
                if subcontract_updated_at_str:
                    subcontract_updated_at = datetime.strptime(
                        subcontract_updated_at_str, config.ce_time_format)

                    # Apply timestamp filter
                    if subcontract_updated_at > last_sync_time:
                        # For initial sync, also apply approval status filter
                        if is_initial_sync:
                            approval_status = subcontract.get(
                                'approval_status', '').lower()
                            if approval_status in config.initial_sync_allowed_statuses:
                                filtered_subcontracts.append(subcontract)
                        else:
                            # For subsequent syncs, include all subcontracts
                            filtered_subcontracts.append(subcontract)

            return filtered_subcontracts

        fetch_computerease_subcontracts = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_subcontracts',
            endpoint='/catalog/subcontract',
            request_method='GET',
            page_limit = 1000,
            data_handler=lambda response: filter_subcontracts_by_timestamp_and_status(
                response.get('data', []) if response else []
            )
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.subcontract_last_sync_time_var,
                value_to_set=rail.result('get_last_sync_time')['current_time']
            )
        )

        has_subcontracts_to_sync = rail.IfOperator(
            task_id='has_subcontracts_to_sync',
            test='{{ result("fetch_computerease_subcontracts") | length > 0 }}',
            yes_task='fetch_ce_cost_types',
            no_task='log_to_sumo'
        )

        fetch_ce_cost_types = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_cost_types',
            endpoint='/catalog/cost-type',
            request_method='GET',
            data_handler=lambda response: {
                str(ct.get('code')): ct.get('reference', '')
                for ct in response.get('data', [])
                if ct.get('code') and ct.get('reference')
            } if response else {}
        )

        fetch_all_procore_projects = rail.ProcoreApiOperator(
            task_id='fetch_all_procore_projects',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            },
            data_handler=lambda projects: {
                p.get('origin_id'): p.get('id')
                for p in (projects or [])
                if p.get('origin_id', '') and p.get('origin_id', '').startswith('CE_')
            }
        )

        fetch_all_procore_vendors = rail.ProcoreApiOperator(
            task_id='fetch_all_procore_vendors',
            endpoint='/vendors',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            },
            data_handler=lambda vendors: {
                v.get('origin_id'): {'id': v.get('id'), 'project_ids': v.get('project_ids', [])}
                for v in (vendors or [])
                if v.get('origin_id', '') and v.get('origin_id', '').startswith('CE_')
            }
        )

        def parse_subcontract_data(subcontracts):
            parsed_subcontracts = []

            for subcontract in subcontracts:
                parsed_subcontracts.append({
                    'code': subcontract.get('code'),
                    'description': subcontract.get('description'),
                    'vendor_code': subcontract.get('vendor', {}).get('code'),
                    'job_code': subcontract.get('job', {}).get('code'),
                    'approval_status': subcontract.get('approval_status'),
                    'retention_percent': subcontract.get('retention_percent'),
                    'contract_text': subcontract.get('contract_text'),
                    'subcontract_item': subcontract.get('subcontract_item', []),
                    'subcontract_event': subcontract.get('subcontract_event', []),

                    'contract_date': subcontract.get('contract_date'),
                    'entered_date': subcontract.get('entered_date'),
                    'approved_date': subcontract.get('approved_date'),
                    'orig_start_date': subcontract.get('orig_start_date'),
                    'orig_finish_date': subcontract.get('orig_finish_date'),
                    'actual_start_date': subcontract.get('actual_start_date'),
                    'actual_finish_date': subcontract.get('actual_finish_date'),
                })
            return parsed_subcontracts

        parse_subcontracts = rail.PythonOperator(
            task_id='parse_subcontracts',
            python_callable=lambda: parse_subcontract_data(
                rail.result('fetch_computerease_subcontracts')
            )
        )

        def group_subcontracts_by_job_func():
            subcontracts = rail.result('parse_subcontracts')
            projects = rail.result('fetch_all_procore_projects') or {}
            vendors = rail.result('fetch_all_procore_vendors') or {}

            groups = {}
            for sub in subcontracts:
                job_code = sub.get('job_code', '') or ''
                vendor_code = sub.get('vendor_code', '') or ''
                project_id = projects.get(f"CE_{job_code}")
                vendor = vendors.get(f"CE_{vendor_code}")

                if job_code not in groups:
                    groups[job_code] = {
                        'job_code': job_code,
                        'procore_project_id': project_id,
                        'subcontracts': []
                    }
                groups[job_code]['subcontracts'].append({
                    'subcontract_data': sub,
                    'procore_vendor': vendor
                })
            return list(groups.values())

        group_subcontracts_by_job = rail.PythonOperator(
            task_id='group_subcontracts_by_job',
            python_callable=group_subcontracts_by_job_func
        )

        trigger_per_project_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_per_project_child_dag',
            items=lambda: rail.result('group_subcontracts_by_job'),
            trigger_dag_id=config.subcontract_per_project_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'job_code': item.get('job_code', ''),
                'procore_project_id': item.get('procore_project_id'),
                'procore_company_id': rail.render_template(procore_company_id_template),
                'ce_cost_type_map': rail.result('fetch_ce_cost_types'),
                'subcontracts': item['subcontracts']
            }
        )

        wait_for_per_project_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_per_project_completion',
            dag_runs='{{ result("trigger_per_project_child_dag") }}',
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
            header=['Subcontract Code', 'Vendor Code',
                    'Job Code', 'Error Message', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('subcontract_code','') }}",
                "{{ item.properties | attr_or_default('vendor_code','') }}",
                "{{ item.properties | attr_or_default('job_code','') }}",
                "{{ item.properties | attr_or_default('error_message','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_logs_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_logs_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_SubcontractSyncLogs_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_notification_with_logs = rail.EmailOperator(
            task_id='send_notification_with_logs',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: Subcontract Sync completed with errors - {{ current_time() }}',
            html_content='/templates/subcontract_sync_logs_notification.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> get_last_sync_time >> fetch_computerease_subcontracts >> set_last_sync_time
        batch_task >> log_to_sumo

        set_last_sync_time >> has_subcontracts_to_sync

        has_subcontracts_to_sync >> rail.Label(
            'Yes') >> fetch_ce_cost_types >> fetch_all_procore_projects >> fetch_all_procore_vendors >> parse_subcontracts
        parse_subcontracts >> group_subcontracts_by_job >> trigger_per_project_child_dag >> wait_for_per_project_completion >> search_logs
        has_subcontracts_to_sync >> rail.Label('No') >> log_to_sumo

        search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_logs_download_link >> send_notification_with_logs >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
