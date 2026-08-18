
from datetime import timedelta
import rail
from statestreet.invoice_update.utils import request_payload

null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'statestreet_invoice_update_vat_calculation_master_{config.instance}',
        description=f'StateStreet Invoice Update - Auto-Calculation of VAT Percentage on Invoice {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:
        

        create_log = rail.CreateLogOperator(
            task_id="create_log"
        )


        #Get Invoice Column URI
        get_sync_status_column_uri = rail.RepliconServiceOperator(
            task_id='get_sync_status_column_uri',
            endpoint='/services/InvoiceListService2.svc/GetAllColumns',
            data_handler=lambda response: next(
                iter(filter(lambda col: col['displayText'] == 'Sync Status', response[0]['columns'])), {}).get('uri', '')
        )


        #GetInvoice filter URI
        get_sync_status_filter_uri = rail.RepliconServiceOperator(
            task_id='get_sync_status_filter_uri',
            endpoint='/services/InvoiceListService2.svc/GetAllFilterDefinitions',
            data_handler=lambda response: next(
                iter(filter(lambda x: x['name'] == 'Sync Status', response)), {}).get('uri', '')
        )



        # Task to get invoice list with "Queued for Sync" status
        get_queued_invoices = rail.RepliconServiceOperator(
            task_id='get_queued_invoices',
            endpoint='/services/InvoiceListService2.svc/GetData',
            data=request_payload.get_queued_invoices_method,
            data_handler=lambda response: {
                'invoices': [
                    {
                        'invoice_uri': row['cells'][0]['uri'],
                        'sync_status': row['cells'][1].get('textValue', '')
                    } for row in response.get('rows', [])
                ]
            }
        )

        # Check if any invoices were found
        has_invoices_to_process = rail.IfOperator(
            task_id='has_invoices_to_process',
            test="{{ result('get_queued_invoices').invoices | length > 0 }}",
            yes_task='process_invoices',
            no_task='log_no_invoices_found'
        )

        log_no_invoices_found = rail.WriteLogOperator(
            task_id='log_no_invoices_found',
            message="No invoices with 'Queued for Sync' status found."
        )

        # Process each invoice
        process_invoices = rail.TriggerDagRunForEachItemOperator(
            task_id='process_invoices',
            thread_pool_size=4,
            retries=0,
            items=lambda: rail.result('get_queued_invoices')['invoices'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'statestreet_invoice_update_vat_calculation_child_{config.instance}',
            conf=lambda item: {
                'invoice_uri': item['invoice_uri'],
                'sync_status': item['sync_status'],
                'vat_percentage': config.vat_percentage,
                'company_key': config.company_key,
                'replicon_conn_id': config.replicon_conn_id
            }
        )

        # Wait for all child DAG runs to complete
        wait_for_invoice_processing = rail.WaitForDagRunsSensor(
            task_id='wait_for_invoice_processing',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_invoices") }}'
        )

        # Gather results from child DAGs
        gather_invoice_results = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_invoice_results',
            dag_runs="{{ result('process_invoices') }}",
            dagrun_task_id='invoice_processing_result',
            flatten=True
        )

        log_invoice_results = rail.WriteLogOperator(
            task_id='log_invoice_results',
            message="{{ result('gather_invoice_results') | tojson }}"
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        # Define task dependencies
        create_log >> get_sync_status_column_uri >> get_sync_status_filter_uri >> get_queued_invoices >> has_invoices_to_process
        has_invoices_to_process >> rail.Label('Yes') >> process_invoices
        has_invoices_to_process >> rail.Label('No') >> log_no_invoices_found >> finish
        
        process_invoices >> wait_for_invoice_processing
        wait_for_invoice_processing >> gather_invoice_results >> log_invoice_results >> finish

    return dag


# Create DAGs for all instances defined in config
rail.for_each_instance(create_dag)