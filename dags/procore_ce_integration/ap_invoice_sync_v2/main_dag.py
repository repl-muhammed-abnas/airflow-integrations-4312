from datetime import datetime, timedelta, timezone
import json
import rail
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable

from procore_ce_integration.ap_invoice_sync_v2.utils import retry_manager, util
from procore_ce_integration.ap_invoice_sync_v2.utils.constants import ErrorType
from procore_ce_integration.initial_setup_sync.shared_utils import build_import_file_description, get_tenant_email


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.ap_invoice_main_dag_id,
        description='Polls for unprocessed and failed events to re/process for sending import file to CE',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs,
        schedule_interval=timedelta(seconds=config.SCHEDULE_INTERVAL_SECONDS),
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        procore_company_id_template = f"{{{{conn.{config.procore_conn_id}.extra_dejson.company_id}}}}"

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
            )
        )

        fetch_webhook_events = rail.S3DownloadFileOperator(
            task_id='fetch_webhook_events',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.ap_invoice_events_key
        )

        fetch_failed_events = rail.S3DownloadFileOperator(
            task_id='fetch_failed_events',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.ap_invoice_failed_events_key
        )

        def _parse_webhook_timestamp(timestamp_str, default_year=1900):
            try:
                return datetime.strptime(timestamp_str, config.procore_webhook_fmt)
            except (TypeError, ValueError):
                return datetime(default_year, 1, 1)

        def _parse_webhook_events(last_sync_time, company_id):
            artifact = rail.result('fetch_webhook_events')
            if not artifact:
                return [], None

            data = rail.load_json_artifact(artifact)
            if not isinstance(data, dict):
                return [], None

            last_processed_dt = _parse_webhook_timestamp(last_sync_time)
            invoices = {}
            max_timestamp = last_processed_dt

            for event_key, event_timestamp in data.get('events', {}).items():
                try:
                    event_dt = _parse_webhook_timestamp(event_timestamp)
                    if event_dt > last_processed_dt:
                        if event_dt > max_timestamp:
                            max_timestamp = event_dt
                        # key format: "{project_id}.{invoice_id}.Draw Requests"
                        parts = event_key.split('.', 2)
                        if len(parts) >= 2:
                            project_id, invoice_id = parts[0], parts[1]
                            invoice_id = int(invoice_id) if invoice_id.isdigit() else invoice_id
                            invoices[event_key] = {
                                'invoice_id': invoice_id,
                                'project_id': project_id,
                                'company_id': company_id,
                            }
                except (ValueError, AttributeError):
                    continue

            max_ts_str = max_timestamp.strftime(config.procore_webhook_fmt) if invoices else None
            return list(invoices.values()), max_ts_str

        def _parse_failed_events():
            artifact = rail.result('fetch_failed_events')
            if not artifact:
                return {}, []

            failed_events = rail.load_json_artifact(artifact)
            if not isinstance(failed_events, dict):
                return {}, []

            retry_attempt_timestamp = datetime.now(timezone.utc).strftime(config.ce_time_format)
            invoices = []
            for entry in failed_events.values():
                entry['last_retried_at'] = retry_attempt_timestamp
                invoices.append({
                    'invoice_id': entry['invoice_id'],
                    'project_id': entry['project_id'],
                    'company_id': entry['company_id'],
                })

            print(f"Injected {len(invoices)} pending retry invoice(s) into this run.")
            return failed_events, invoices

        def prepare_invoices_to_sync():
            last_sync_time = rail.result('get_last_sync_time')['last_synctime']
            company_id = rail.render_template(procore_company_id_template)

            webhook_invoices, max_webhook_timestamp = _parse_webhook_events(last_sync_time, company_id)
            failed_events, failed_invoices = _parse_failed_events()

            distinct_invoices = {}
            for inv in webhook_invoices + failed_invoices:
                distinct_invoices.setdefault(str(inv['invoice_id']), inv)
            all_invoices = list(distinct_invoices.values())

            # Group by project_id — one child DAG run per project
            project_groups = {}
            for inv in all_invoices:
                pid = str(inv['project_id'])
                if pid not in project_groups:
                    project_groups[pid] = {
                        'project_id': inv['project_id'],
                        'company_id': inv['company_id'],
                        'invoice_ids': []
                    }
                project_groups[pid]['invoice_ids'].append(inv['invoice_id'])

            valid_groups = [
                g for g in project_groups.values()
                if g.get('project_id') and g.get('invoice_ids')
            ]

            return {
                'invoices': valid_groups,
                'invoice_by_id': {str(inv['invoice_id']): inv for inv in all_invoices},
                'failed_events': failed_events,
                'max_webhook_timestamp': max_webhook_timestamp
            }

        prepare_invoices = rail.PythonOperator(
            task_id='prepare_invoices',
            python_callable=prepare_invoices_to_sync
        )

        def _resolve_sync_watermark():
            prepare_result = rail.result('prepare_invoices')
            max_ts = prepare_result.get('max_webhook_timestamp')
            if max_ts:
                return max_ts
            return rail.result('get_last_sync_time')['last_synctime']

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                value_to_set=_resolve_sync_watermark()
            )
        )

        has_invoices = rail.IfOperator(
            task_id='has_invoices',
            test='{{ result("prepare_invoices").invoices | length > 0 }}',
            yes_task='trigger_invoice_child_dags',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        # Trigger child DAG per project
        trigger_invoice_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_invoice_child_dags',
            items='{{ result("prepare_invoices").invoices | to_json }}',
            trigger_dag_id=config.ap_invoice_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: item
        )

        wait_for_child_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_completion',
            dag_runs='{{ result("trigger_invoice_child_dags") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Gather results from all child DAG runs
        collect_invoice_data = rail.GatherResultsFromDagRunsOperator(
            task_id='collect_invoice_data',
            dag_runs='{{ result("trigger_invoice_child_dags") }}',
            dagrun_task_id='prepare_invoice_data'
        )

        def segregate_invoice_results():
            invoice_by_id = rail.result('prepare_invoices')['invoice_by_id']
            existing_failed_events = rail.result('prepare_invoices')['failed_events']
            child_results = rail.result('collect_invoice_data')

            # Flatten: each child DAG now returns a list of invoice results
            child_results_flat = []
            for item in child_results:
                if isinstance(item, list):
                    child_results_flat.extend(item)
                elif item:
                    child_results_flat.append(item)

            child_results_by_invoice_id = {
                str(r['invoice_id']): r for r in child_results_flat
                if r and r.get('invoice_id') is not None
            }

            valid_invoices, errors, skipped, record_not_found_invoices = [], [], [], []

            for inv in invoice_by_id.values():
                invoice_id = inv.get('invoice_id')
                child_dag_result = child_results_by_invoice_id.get(str(invoice_id))

                if child_dag_result is None:
                    errors.append({
                        'invoice_id': invoice_id,
                        'invoice_number': '',
                        'error_message': 'Invoice could not be processed due to unknown error',
                        'error_type': 'Processing Error',
                    })
                elif 'error' in child_dag_result:
                    error_entry = {
                        'invoice_id': invoice_id,
                        'invoice_number': child_dag_result.get('invoice_number', ''),
                        'error_message': f"Invoice sync skipped: {child_dag_result['error']}",
                        'error_type': child_dag_result.get('error_type', 'Unknown'),
                    }
                    if error_entry['error_type'] == ErrorType.API_ERROR:
                        record_not_found_invoices.append(error_entry)
                    else:
                        errors.append(error_entry)
                elif child_dag_result.get('skipped'):
                    skipped.append({
                        'invoice_id': invoice_id,
                        'ce_status': child_dag_result.get('ce_status'),
                        'import_uuid': child_dag_result.get('import_uuid'),
                    })
                else:
                    valid_invoices.append(child_dag_result)

            successfully_processed_invoice_ids = (
                {inv['invoice_id'] for inv in valid_invoices} |
                {inv['invoice_id'] for inv in skipped}
            )

            updated_pending_retries, exhausted_entries = retry_manager.update_retry_queue(
                record_not_found_invoices,
                successfully_processed_invoice_ids,
                errors,
                invoice_by_id,
                existing_failed_events,
                config.MAX_RETRY_ATTEMPTS,
            )
            for exhausted_entry in exhausted_entries:
                errors.append({
                    'invoice_id': exhausted_entry['invoice_id'],
                    'invoice_number': exhausted_entry.get('invoice_number', ''),
                    'error_message': (
                        f"Invoice could not be synced after {exhausted_entry['retry_count']} attempts "
                        f"and has been removed from the retry queue."
                    ),
                    'error_type': 'Max Retries Exceeded',
                })

            return {
                'valid_invoices': valid_invoices,
                'errors': errors,
                'skipped': skipped,
                'updated_failed_events_json': json.dumps(updated_pending_retries, separators=(',', ':'))
            }

        segregate_results = rail.PythonOperator(
            task_id='segregate_results',
            python_callable=segregate_invoice_results
        )

        upload_failed_events_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_failed_events_to_s3',
            aws_conn_id=config.aws_conn_id,
            source='{{ result("segregate_results").updated_failed_events_json }}',
            bucket_name=config.s3_bucket_name,
            key_name=config.ap_invoice_failed_events_key,
            replace=True
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test='{{ result("segregate_results").valid_invoices | length > 0 }}',
            yes_task='generate_xml_for_invoices',
            no_task='check_for_errors'
        )

        def generate_xml_for_all_invoices():
            segregated_data = rail.result('segregate_results')
            return util.generate_xml_payload(
                segregated_data.get('valid_invoices', []),
                segregated_data.get('errors', []),
            )

        generate_xml_for_invoices = rail.PythonOperator(
            task_id='generate_xml_for_invoices',
            python_callable=generate_xml_for_all_invoices
        )

        create_invoices_in_ce = rail.ComputereaseAPIOperator(
            task_id='create_invoices_in_ce',
            endpoint='/import/',
            request_method='POST',
            request_body=lambda: {
                "import_type": "Payable Invoices",
                "description": build_import_file_description(
                    "AP Invoice",
                    ", ".join(
                        inv.get('data', {}).get('invoice_number', str(inv.get('invoice_id', '')))
                        for inv in rail.result('segregate_results', {}).get('valid_invoices', [])
                    )
                ),
                "import_data": rail.result('generate_xml_for_invoices')['import_data']
            },
            data_handler=lambda response: {
                'uuid': response.get('data', {}).get('uuid') if response and response.get('data') else None,
                'raw_response': response
            }
        )

        for_each_valid_invoice = rail.ForEachOperator(
            task_id='for_each_valid_invoice',
            items=lambda: rail.result('segregate_results', {}).get('valid_invoices', []),
            start_task='update_invoice_origin_data',
            end_task='for_each_valid_invoice_end'
        )

        def get_origin_data_update_payload():
            import_uuid = rail.result('create_invoices_in_ce', {}).get('uuid')
            current_invoice_data = rail.result('for_each_valid_invoice')
            invoice = current_invoice_data.get('data', {})
            invoice_id = current_invoice_data.get('invoice_id')

            invoice_by_id = rail.result('prepare_invoices', {}).get('invoice_by_id', {})
            project_id = invoice_by_id.get(str(invoice_id), {}).get('project_id', '')

            # origin_data (import_uuid) always written for re-send dedup; origin_id
            # is deferred to the mark ERP sync DAG (set after CE accepts) when enabled.
            requisition = {'origin_data': json.dumps({'import_uuid': import_uuid})}
            if not config.defer_origin_id_until_accepted:
                requisition['origin_id'] = 'CE_' + invoice.get('invoice_number', '')

            return {
                'project_id': project_id,
                'commitment_id': invoice.get('commitment_id'),
                'requisition': requisition
            }

        update_invoice_origin_data = rail.ProcoreApiOperator(
            task_id='update_invoice_origin_data',
            endpoint=lambda: f"/requisitions/{rail.result('for_each_valid_invoice').get('invoice_id')}",
            method='PATCH',
            data=get_origin_data_update_payload
        )

        for_each_valid_invoice_end = rail.EmptyOperator(
            task_id='for_each_valid_invoice_end'
        )

        if_defer_enabled = rail.IfOperator(
            task_id='if_defer_enabled',
            test=lambda: config.defer_origin_id_until_accepted,
            yes_task='build_worklist_rows',
            no_task='check_for_errors'
        )

        def build_worklist_rows():
            import_uuid = rail.result('create_invoices_in_ce', {}).get('uuid')
            valid_invoices = rail.result('segregate_results', {}).get('valid_invoices', [])
            invoice_by_id = rail.result('prepare_invoices', {}).get('invoice_by_id', {})
            queued_at = rail.render_template('{{ current_time() }}')
            rows = []
            for inv in valid_invoices:
                data = inv.get('data', {})
                invoice_id = inv.get('invoice_id')
                if not (invoice_id and import_uuid):
                    continue
                rows.append({
                    'invoice_id': str(invoice_id),
                    'invoice_number': data.get('invoice_number', ''),
                    'project_id': str(invoice_by_id.get(str(invoice_id), {}).get('project_id', '')),
                    'commitment_id': str(data.get('commitment_id', '') or ''),
                    'import_uuid': import_uuid or '',
                    'queued_at': queued_at
                })
            return rows

        build_worklist_rows_task = rail.PythonOperator(
            task_id='build_worklist_rows',
            python_callable=build_worklist_rows
        )

        enqueue_pending = rail.S3UpsertCollectionOperator(
            task_id='enqueue_pending',
            integration=config.s3_collection['integration'],
            customer=config.instance,
            collection_name=config.origin_id_update_table['name'],
            key_columns=config.origin_id_update_table['unique_columns'],
            rows=build_worklist_rows_task.output
        )

        check_for_errors = rail.IfOperator(
            task_id='check_for_errors',
            test=lambda: (
                len(rail.result('segregate_results', {}).get('errors', [])) > 0 or
                (rail.result('generate_xml_for_invoices', None) is not None and
                 len(rail.result('generate_xml_for_invoices', {}).get('errors', [])) > 0)
            ),
            yes_task='prepare_final_errors',
            no_task='log_to_sumo'
        )

        def prepare_final_errors():
            if rail.result('generate_xml_for_invoices', None) is not None:
                return rail.result('generate_xml_for_invoices').get('errors', [])
            segregate_result = rail.result('segregate_results', {})
            return segregate_result.get('errors', [])

        prepare_final_errors_task = rail.PythonOperator(
            task_id='prepare_final_errors',
            python_callable=prepare_final_errors
        )

        write_errors_to_csv = rail.WriteCSVFileOperator(
            task_id='write_errors_to_csv',
            source='{{ result("prepare_final_errors") | to_json }}',
            header=['Invoice ID', 'Invoice Number',
                    'Error Type', 'Error Message'],
            row=[
                "{{ item.invoice_id }}",
                "{{ item.invoice_number }}",
                "{{ item.error_type }}",
                "{{ item.error_message }}"
            ]
        )

        generate_error_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_error_download_link',
            artifact_name='{{ result("write_errors_to_csv") }}',
            output_file_name='ProCore_CE_APInvoiceSync_Errors_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_error_notification = rail.EmailOperator(
            task_id='send_error_notification',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: AP Invoice Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/ap_invoice_sync_error.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> log_to_sumo
        batch_task >> get_last_sync_time
        get_last_sync_time >> fetch_webhook_events >> fetch_failed_events >> prepare_invoices

        prepare_invoices >> set_last_sync_time >> has_invoices
        has_invoices >> rail.Label('Yes') >> trigger_invoice_child_dags >> wait_for_child_completion >> collect_invoice_data >> segregate_results
        has_invoices >> rail.Label('No') >> delete_this_dagrun

        segregate_results >> upload_failed_events_to_s3 >> has_valid_data

        has_valid_data >> rail.Label(
            'Yes') >> generate_xml_for_invoices >> create_invoices_in_ce >> for_each_valid_invoice >> for_each_valid_invoice_end
        for_each_valid_invoice >> update_invoice_origin_data >> for_each_valid_invoice_end >> if_defer_enabled
        if_defer_enabled >> rail.Label(
            'Yes') >> build_worklist_rows_task >> enqueue_pending >> check_for_errors
        if_defer_enabled >> rail.Label('No') >> check_for_errors
        has_valid_data >> rail.Label('No') >> check_for_errors

        check_for_errors >> rail.Label(
            'Yes') >> prepare_final_errors_task >> write_errors_to_csv >> generate_error_download_link >> send_error_notification >> log_to_sumo
        check_for_errors >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
