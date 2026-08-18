from datetime import timedelta
import base64
import io
import zipfile
import json
import rail
from procore_ce_integration.ar_invoice_sync.utils.xml_generator import generate_ce_ar_invoice_xml, combine_ar_invoice_xmls
from procore_ce_integration.initial_setup_sync.shared_utils import build_import_file_description, get_tenant_email


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.ar_invoice_main_dag_id,
        description='Procore to Computerease AR Invoice Sync MAIN DAG',
        max_active_runs=config.max_active_runs,
        integration_type='generic',
        company_key=config.instance,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
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

        def prepare_ar_invoices_to_sync(dag_run):
            conf = dag_run.conf
            if 'webhook' in conf:
                webhook_data = conf['webhook']['data']
                if webhook_data['resource_name'] == 'Payment Applications':
                    return [{
                        'invoice_id': webhook_data['resource_id'],
                        'project_id': webhook_data['project_id'],
                        'company_id': webhook_data['company_id'],
                        'event_type': webhook_data['event_type']
                    }]
            return []

        prepare_invoices = rail.PythonOperator(
            task_id='prepare_invoices',
            python_callable=prepare_ar_invoices_to_sync
        )

        has_invoices = rail.IfOperator(
            task_id='has_invoices',
            test='{{ result("prepare_invoices") | length > 0 }}',
            yes_task='trigger_invoice_child_dags',
            no_task='log_to_sumo'
        )

        trigger_invoice_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_invoice_child_dags',
            items='{{ result("prepare_invoices") | to_json }}',
            trigger_dag_id=config.ar_invoice_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: item
        )

        wait_for_child_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_completion',
            dag_runs='{{ result("trigger_invoice_child_dags") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        collect_invoice_data = rail.GatherResultsFromDagRunsOperator(
            task_id='collect_invoice_data',
            dag_runs='{{ result("trigger_invoice_child_dags") }}',
            dagrun_task_id='prepare_ar_invoice_data'
        )

        def segregate_ar_invoice_results():
            """
            Iterate over initial invoices and segregate based on child DAG results.
            Returns dict with 'valid_invoices', 'errors', and 'skipped' arrays.
            """
            initial_invoices = rail.result('prepare_invoices')
            child_results = rail.result('collect_invoice_data')

            # Create a map for quick lookup
            results_map = {}
            for result in child_results:
                if result and 'invoice_id' in result:
                    results_map[result['invoice_id']] = result

            valid_invoices = []
            errors = []
            skipped = []

            for invoice in initial_invoices:
                invoice_id = invoice.get('invoice_id')

                if invoice_id in results_map:
                    child_result = results_map[invoice_id]

                    if 'error' in child_result:
                        errors.append({
                            'invoice_id': invoice_id,
                            'invoice_number': child_result.get('invoice_number', ''),
                            'error_message': f"AR Invoice sync skipped: {child_result['error']}",
                            'error_type': child_result.get('error_type', 'Unknown'),
                        })
                    elif child_result.get('skipped'):
                        if child_result.get('should_log', False):
                            errors.append({
                                'invoice_id': invoice_id,
                                'invoice_number': child_result.get('invoice_number', ''),
                                'error_message': child_result.get('reason', 'Skipped'),
                                'error_type': 'Skipped'
                            })
                        else:
                            skipped.append({
                                'invoice_id': invoice_id,
                                'ce_status': child_result.get('ce_status', 'N/A'),
                                'import_uuid': child_result.get('import_uuid', 'N/A'),
                                'reason': child_result.get('reason', 'Skipped')
                            })
                    else:
                        # Valid invoice with data
                        valid_invoices.append(child_result)
                else:
                    errors.append({
                        'invoice_id': invoice_id,
                        'invoice_number': '',
                        'error_message': 'AR Invoice could not be processed due to unknown error',
                        'error_type': 'Processing Error',
                    })

            return {
                'valid_invoices': valid_invoices,
                'errors': errors,
                'skipped': skipped
            }

        segregate_results = rail.PythonOperator(
            task_id='segregate_results',
            python_callable=segregate_ar_invoice_results
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test='{{ result("segregate_results").valid_invoices | length > 0 }}',
            yes_task='generate_xml_for_invoices',
            no_task='check_for_errors'
        )

        def zip_and_base64_encode_xml(xml_str):
            if not xml_str:
                return None

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr('ar_invoices.xml', xml_str)

            zip_data = zip_buffer.getvalue()
            return base64.b64encode(zip_data).decode('utf-8')

        def generate_xml_for_all_ar_invoices():
            """
            Generate XML for all valid AR invoices.
            Returns dict with 'xml' key containing final XML and 'errors' array.
            """
            segregated_data = rail.result('segregate_results')
            valid_invoices = segregated_data.get('valid_invoices', [])
            errors = segregated_data.get('errors', [])

            xml_invoices = []

            for invoice_data in valid_invoices:
                invoice_id = invoice_data.get('invoice_id')

                try:
                    # Generate XML for this AR invoice
                    xml_content = generate_ce_ar_invoice_xml(
                        invoice_data.get('data', invoice_data))
                    xml_invoices.append(xml_content)

                except Exception as e:
                    errors.append({
                        'invoice_id': invoice_id,
                        'invoice_number': invoice_data.get('data', {}).get('invoice_number', '') if invoice_data else '',
                        'error_message': str(e),
                        'error_type': 'XML Generation'
                    })

            # Combine all AR invoice XMLs into final XML using proper XML structure
            final_xml = combine_ar_invoice_xmls(
                xml_invoices) if xml_invoices else None

            return {
                'xml': final_xml,
                'import_data': zip_and_base64_encode_xml(final_xml),
                'errors': errors
            }

        generate_xml_for_invoices = rail.PythonOperator(
            task_id='generate_xml_for_invoices',
            python_callable=generate_xml_for_all_ar_invoices
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
            output_file_name='ProCore_CE_ARInvoiceSync_Errors_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_error_notification = rail.EmailOperator(
            task_id='send_error_notification',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: AR Invoice Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/ar_invoice_sync_error.html'
        )

        create_invoices_in_ce = rail.ComputereaseAPIOperator(
            task_id='create_invoices_in_ce',
            endpoint='/import/',
            request_method='POST',
            request_body=lambda: {
                "import_type": "Receivable Invoices",
                "description": build_import_file_description(
                    "AR Invoice",
                    ", ".join(
                        inv.get('invoice_number', str(inv.get('invoice_id', '')))
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

        def get_origin_data_update_payload():
            origin_data = {'import_uuid': rail.result(
                'create_invoices_in_ce', {}).get('uuid')}
            origin_data_string = json.dumps(origin_data)
            return {
                'project_id': rail.result('prepare_invoices')[0].get('project_id'),
                'payment_application': {
                    'origin_data': origin_data_string
                }
            }

        # Update origin_data for the Payment Application
        update_payment_app_origin_data = rail.ProcoreApiOperator(
            task_id='update_payment_app_origin_data',
            endpoint=lambda: f"/prime_contracts/{rail.result('collect_invoice_data')[0].get('data', {}).get('contract_id')}/payment_applications/{rail.result('prepare_invoices')[0].get('invoice_id')}",
            method='PATCH',
            data=get_origin_data_update_payload
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
        has_invoices >> rail.Label('No') >> log_to_sumo

        has_valid_data >> rail.Label(
            'Yes') >> generate_xml_for_invoices >> create_invoices_in_ce >> update_payment_app_origin_data >> check_for_errors
        has_valid_data >> rail.Label('No') >> check_for_errors

        check_for_errors >> rail.Label(
            'Yes') >> prepare_final_errors_task >> write_errors_to_csv >> generate_error_download_link >> send_error_notification >> log_to_sumo
        check_for_errors >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
