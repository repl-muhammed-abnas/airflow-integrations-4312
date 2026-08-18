from datetime import timedelta, datetime, timezone
import rail
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from procore_ce_integration.initial_setup_sync.shared_utils import get_tenant_email


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.job_structure_main_dag_id,
        description='Procore Job Structure Webhook Events Processing - Main DAG',
        integration_type='generic',
        company_key=config.instance,
        schedule_interval=timedelta(seconds=config.schedule_seconds),
        max_active_runs=config.max_active_runs,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id
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
                variable_name=config.webhook_events_last_sync_time_var,
                date_format=config.procore_webhook_fmt,
                initial_sync_time='1900-01-01T00:00:00.000Z',
                reset_after_threshold=False,
                use_param_date_format=True
            ),
        )

        fetch_webhook_events = rail.S3DownloadFileOperator(
            task_id='fetch_webhook_events',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.job_structure_events_key
        )

        def _load_webhook_events():
            try:
                events_data = rail.load_json_artifact(
                    rail.result('fetch_webhook_events'))
                if not isinstance(events_data, dict):
                    return None
                return events_data
            except Exception:
                return None

        def _parse_webhook_timestamp(timestamp_str, default_year=1900):
            try:
                return datetime.strptime(timestamp_str, config.procore_webhook_fmt)
            except:
                return datetime(default_year, 1, 1, tzinfo=timezone.utc)

        def parse_webhook_events():

            events_data = _load_webhook_events()
            if not events_data:
                return {'projects_to_sync': [], 'total_projects': 0, 'max_processed_timestamp': None}

            last_processed_timestamp = rail.result(
                'get_last_sync_time')['last_synctime']
            last_processed_dt = _parse_webhook_timestamp(
                last_processed_timestamp)

            projects_with_changes = {}
            max_processed_timestamp = last_processed_dt  # Track highest timestamp processed

            for event_key, event_timestamp in events_data.items():  # pylint: disable=too-many-nested-blocks
                if event_key == '_metadata':
                    continue
                try:
                    event_dt = _parse_webhook_timestamp(event_timestamp)
                    if event_dt > last_processed_dt:
                        # Track the highest timestamp we're processing
                        if event_dt > max_processed_timestamp:
                            max_processed_timestamp = event_dt

                        if '.' in event_key:
                            project_id, resource_id, resource_type = event_key.split(
                                '.', 2)
                            cost_code_id = ''
                            budget_line_item_id = ''
                            has_prime_contract = False
                            if resource_type == 'Budget Line Items':
                                budget_line_item_id = resource_id
                            elif resource_type == 'Prime Contracts':
                                has_prime_contract = True
                            else:
                                cost_code_id = resource_id

                            if project_id not in projects_with_changes:
                                projects_with_changes[project_id] = {
                                    'project_id': project_id,
                                    'has_project_changes': False,
                                    'cost_code_ids': [cost_code_id] if cost_code_id else [],
                                    'budget_line_item_ids': [budget_line_item_id] if budget_line_item_id else [],
                                    'has_prime_contract': has_prime_contract
                                }
                            else:
                                if cost_code_id and cost_code_id not in projects_with_changes[project_id]['cost_code_ids']:
                                    projects_with_changes[project_id]['cost_code_ids'].append(
                                        cost_code_id)
                                if budget_line_item_id and budget_line_item_id not in projects_with_changes[project_id]['budget_line_item_ids']:
                                    projects_with_changes[project_id]['budget_line_item_ids'].append(
                                        budget_line_item_id)
                                if has_prime_contract:
                                    projects_with_changes[project_id]['has_prime_contract'] = True
                        else:  # Project event: "project_id"
                            project_id = event_key
                            if project_id not in projects_with_changes:
                                projects_with_changes[project_id] = {
                                    'project_id': project_id,
                                    'has_project_changes': True,
                                    'cost_code_ids': [],
                                    'budget_line_item_ids': [],
                                    'has_prime_contract': False
                                }
                            else:
                                projects_with_changes[project_id]['has_project_changes'] = True
                except (ValueError, AttributeError):
                    continue

            projects_to_sync = list(projects_with_changes.values())

            return {
                'projects_to_sync': projects_to_sync,
                'total_projects': len(projects_to_sync),
                'max_processed_timestamp': max_processed_timestamp.strftime(
                    config.procore_webhook_fmt)
            }

        process_webhook_events = rail.PythonOperator(
            task_id='process_webhook_events',
            python_callable=parse_webhook_events
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                value_to_set=rail.result('process_webhook_events')[
                    'max_processed_timestamp'] if rail.result('process_webhook_events') and rail.result('process_webhook_events')['max_processed_timestamp'] else rail.result('get_last_sync_time')['current_time']
            )
        )

        check_if_projects_to_sync = rail.IfOperator(
            task_id='check_if_projects_to_sync',
            test=lambda: rail.result('process_webhook_events') and rail.result(
                'process_webhook_events')['total_projects'] > 0,
            yes_task='fetch_cost_code_segments',
            no_task='log_to_sumo'
        )

        fetch_cost_code_segments = rail.ProcoreApiOperator(
            task_id='fetch_cost_code_segments',
            endpoint=lambda: f'/companies/{rail.render_template(procore_company_id_template)}/work_breakdown_structure/segments',
            method='GET',
            data_handler=lambda segments: next(
                (seg['id'] for seg in segments
                 if seg.get('type') == 'cost_code' and seg.get('structure') == 'tiered'),
                None
            )
        )

        trigger_project_sync_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_sync_child_dags',
            items='{{ result("process_webhook_events").projects_to_sync | to_json }}',
            trigger_dag_id=config.job_structure_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'project_data': {
                    **item,
                    'has_prime_contract': True,
                    'should_do_full_sync': True
                },
                'company_id': rail.render_template(procore_company_id_template),
                'cost_code_segment_id': rail.result('fetch_cost_code_segments'),
                'sync_all_cost_codes': True
            }
        )

        wait_for_child_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_completion',
            dag_runs='{{ result("trigger_project_sync_child_dags") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_project_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_project_data',
            dag_runs='{{ result("trigger_project_sync_child_dags") }}',
            dagrun_task_id='build_project_context'
        )

        def build_bulk_update_payload():
            gathered_results = rail.result('gather_project_data')
            defer = config.defer_origin_id_until_accepted
            seen = {}

            for result in gathered_results:
                if result and result.get('procore_project_id') and result.get('origin_id'):
                    # New/changed links are set by the origin_id update DAG after
                    # CE accepts; only already-correct links pass through here.
                    if defer and result.get('existing_origin_id', '') != result['origin_id']:
                        continue
                    origin_id = result['origin_id']
                    if origin_id in seen:
                        seen[origin_id] = None  # duplicate — skip both
                    else:
                        seen[origin_id] = {
                            'id': int(result['procore_project_id']),
                            'origin_id': origin_id,
                            'project_number': result.get('project_number', '')
                        }

            return {
                "company_id": rail.render_template(procore_company_id_template),
                "updates": [u for u in seen.values() if u is not None]
            }

        get_bulk_update_payload = rail.PythonOperator(
            task_id='get_bulk_update_payload',
            python_callable=build_bulk_update_payload
        )

        if_update_needed = rail.IfOperator(
            task_id='if_update_needed',
            test='{{ result("get_bulk_update_payload").updates | length > 0 }}',
            yes_task='bulk_update_project_origins',
            no_task='search_logs'
        )

        bulk_update_project_origins = rail.ProcoreApiOperator(
            task_id='bulk_update_project_origins',
            endpoint='/projects/sync',
            method='PATCH',
            data=lambda: rail.result('get_bulk_update_payload')
        )

        def build_origin_id_update_rows():
            if not config.defer_origin_id_until_accepted:
                return []
            records = rail.result('gather_project_data') or []
            queued_at = rail.render_template('{{ current_time() }}')
            rows, seen = [], set()
            for record in records:
                if not record:
                    continue
                origin_id = record.get('origin_id')
                project_number = record.get('project_number')
                import_uuid = record.get('import_uuid')
                if not (origin_id and project_number and import_uuid):
                    continue
                if record.get('existing_origin_id', '') == origin_id:
                    continue  # already linked — nothing to defer
                if project_number in seen:
                    continue
                seen.add(project_number)
                rows.append({
                    'project_number': project_number,
                    'procore_project_id': str(record.get('procore_project_id', '')),
                    'origin_id': origin_id,
                    'import_uuid': import_uuid,
                    'queued_at': queued_at
                })
            return rows

        build_origin_id_update_rows_task = rail.PythonOperator(
            task_id='build_origin_id_update_rows',
            python_callable=build_origin_id_update_rows
        )

        if_defer_enabled = rail.IfOperator(
            task_id='if_defer_enabled',
            test=lambda: config.defer_origin_id_until_accepted,
            yes_task='build_origin_id_update_rows',
            no_task='get_bulk_update_payload'
        )

        enqueue_pending = rail.S3UpsertCollectionOperator(
            task_id='enqueue_pending',
            integration=config.s3_collection['integration'],
            customer=config.instance,
            collection_name=config.origin_id_update_table['name'],
            key_columns=config.origin_id_update_table['unique_columns'],
            rows=build_origin_id_update_rows_task.output
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test='{{ result("search_logs", "length") > 0 }}',
            yes_task='write_logs_csv',
            no_task='log_to_sumo'
        )

        write_logs_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_csv',
            source='{{ result("search_logs") }}',
            header=['Entity', 'Code', 'Procore Project ID',
                    'Project Name', 'Budget IDs', 'Message', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('entity_type','') }}",
                "{{ item.properties | attr_or_default('entity_code','') }}",
                "{{ item.properties | attr_or_default('procore_project_id','') }}",
                "{{ item.properties | attr_or_default('procore_project_name','') }}",
                "{{ item.properties | attr_or_default('budget_line_item_ids','') }}",
                "{{ item.properties | attr_or_default('error_message','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_logs_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_logs_download_link',
            artifact_name='{{ result("write_logs_csv") }}',
            output_file_name='Procore_to_Computerease_Job_Structure_Sync_Logs_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_notification_with_logs = rail.EmailOperator(
            task_id='send_notification_with_logs',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: Job Structure Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/job_sync_logs_notification.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> get_last_sync_time >> fetch_webhook_events >> process_webhook_events >> set_last_sync_time >> check_if_projects_to_sync

        check_if_projects_to_sync >> rail.Label(
            'Yes') >> fetch_cost_code_segments >> trigger_project_sync_child_dags >> wait_for_child_completion
        wait_for_child_completion >> gather_project_data >> if_defer_enabled
        if_defer_enabled >> rail.Label('Yes') >> \
            build_origin_id_update_rows_task >> enqueue_pending >> get_bulk_update_payload
        if_defer_enabled >> rail.Label('No') >> get_bulk_update_payload
        get_bulk_update_payload >> if_update_needed
        check_if_projects_to_sync >> rail.Label('No') >> log_to_sumo

        if_update_needed >> rail.Label(
            'Yes') >> bulk_update_project_origins >> search_logs
        if_update_needed >> rail.Label('No') >> search_logs

        search_logs >> if_logs_present
        if_logs_present >> rail.Label(
            'Yes') >> write_logs_csv >> generate_logs_download_link >> send_notification_with_logs >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        batch_task >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
