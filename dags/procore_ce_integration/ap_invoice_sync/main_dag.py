from datetime import datetime, timedelta, timezone
import json
import rail

from procore_ce_integration.ap_invoice_sync.utils import retry_manager, util
from procore_ce_integration.ap_invoice_sync.utils.constants import ErrorType
from procore_ce_integration.initial_setup_sync.shared_utils import build_import_file_description, get_tenant_email


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.ap_invoice_main_dag_id,
        description='Procore to Computerease AP Invoice Sync MAIN DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs,
        schedule_interval=timedelta(seconds=config.SCHEDULE_INTERVAL_SECONDS),
        webhook_conf=rail.WebhookConf(bearer_token_var=config.bearer_token_var),
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:


        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='prepare_invoices',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def prepare_invoices_to_sync(dag_run):
            conf = dag_run.conf
            invoices = []

            if 'webhook' in conf:
                webhook_data = conf['webhook']['data']
                if webhook_data['resource_name'] == 'Draw Requests':
                    invoices.append({
                        'invoice_id': webhook_data['resource_id'],
                        'project_id': webhook_data['project_id'],
                        'company_id': webhook_data['company_id'],
                        'event_type': webhook_data['event_type']
                    })
            else:
                pending_retries = retry_manager.load(config.failed_invoices_var)
                if pending_retries:
                    retry_attempt_timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                    for pending_invoice in pending_retries.values():
                        pending_invoice['last_retried_at'] = retry_attempt_timestamp
                        invoices.append({
                            'invoice_id': pending_invoice['invoice_id'],
                            'project_id': pending_invoice['project_id'],
                            'company_id': pending_invoice['company_id'],
                        })
                    retry_manager.save(config.failed_invoices_var, pending_retries)
                    print(f"Injected {len(pending_retries)} pending retry invoice(s) into this run.")

            return invoices

        prepare_invoices = rail.PythonOperator(
            task_id='prepare_invoices',
            python_callable=prepare_invoices_to_sync
        )

        has_invoices = rail.IfOperator(
            task_id='has_invoices',
            test='{{ result("prepare_invoices") | length > 0 }}',
            yes_task='trigger_invoice_child_dags',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        # Trigger child DAG for each invoice
        trigger_invoice_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_invoice_child_dags',
            items='{{ result("prepare_invoices") | to_json }}',
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
            initial_invoices = rail.result('prepare_invoices')
            child_results = rail.result('collect_invoice_data')

            child_results_by_invoice_id = {r['invoice_id']: r for r in child_results if r and 'invoice_id' in r}
            initial_invoice_by_id = {inv['invoice_id']: inv for inv in initial_invoices}

            valid_invoices, errors, skipped, record_not_found_invoices = [], [], [], []

            for invoice in initial_invoices:
                invoice_id = invoice.get('invoice_id')
                child_dag_result = child_results_by_invoice_id.get(invoice_id)

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

            exhausted_entries = retry_manager.update_retry_queue(
                record_not_found_invoices,
                successfully_processed_invoice_ids,
                errors,
                initial_invoice_by_id,
                config.failed_invoices_var,
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
                'skipped': skipped
            }

        segregate_results = rail.PythonOperator(
            task_id='segregate_results',
            python_callable=segregate_invoice_results
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

            project_id_by_invoice_id = {
                inv['invoice_id']: inv['project_id']
                for inv in rail.result('prepare_invoices', [])
            }

            return {
                'project_id': project_id_by_invoice_id.get(invoice_id, ''),
                'commitment_id': invoice.get('commitment_id'),
                'requisition': {
                    'origin_id': 'CE_' + invoice.get('invoice_number', ''),
                    'origin_data': json.dumps({'import_uuid': import_uuid})
                }
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
            # XML generation was skipped, use segregate errors directly
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
        batch_task >> prepare_invoices >> has_invoices
        batch_task >> log_to_sumo

        has_invoices >> rail.Label(
            'Yes') >> trigger_invoice_child_dags >> wait_for_child_completion >> collect_invoice_data >> segregate_results >> has_valid_data
        has_invoices >> rail.Label('No') >> delete_this_dagrun

        has_valid_data >> rail.Label(
            'Yes') >> generate_xml_for_invoices >> create_invoices_in_ce >> for_each_valid_invoice >> for_each_valid_invoice_end
        for_each_valid_invoice >> update_invoice_origin_data >> for_each_valid_invoice_end >> check_for_errors
        has_valid_data >> rail.Label('No') >> check_for_errors

        check_for_errors >> rail.Label(
            'Yes') >> prepare_final_errors_task >> write_errors_to_csv >> generate_error_download_link >> send_error_notification >> log_to_sumo
        check_for_errors >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)

