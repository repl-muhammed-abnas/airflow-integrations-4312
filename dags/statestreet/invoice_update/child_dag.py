
import rail, uuid
from statestreet.invoice_update.utils import request_payload

null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'statestreet_invoice_update_vat_calculation_child_{config.instance}',
        description=f'StateStreet Invoice Update - Auto-Calculation of VAT Percentage - Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1 
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",
            extra_config=config)

        # Get invoice details
        get_invoice_details = rail.RepliconServiceOperator(
            task_id='get_invoice_details',
            endpoint='/services/InvoiceService2.svc/GetInvoiceDetails',
            data=lambda dag_run : {
                'invoiceUri': dag_run.conf['invoice_uri']
            },
            data_handler=lambda response, dag_run:{
                'invoice': response,
                'invoice_uri': dag_run.conf['invoice_uri'],
                'invoice_status': response.get('invoiceStatus', {}),
                'invoice_number_text': response.get('invoiceNumberText', {})
            }
        )

        # Get invoice items
        get_invoice_items = rail.RepliconServiceOperator(
            task_id='get_invoice_items',
            endpoint='/services/InvoiceService2.svc/GetPageOfInvoiceItemsForInvoice3',
            data=request_payload.get_invoice_items_method,
            data_handler=lambda response: {
                'invoice_items': response,
                'item_count': len(response) if response else 0
            }
        )

        # Check if invoice has line items
        has_line_items = rail.IfOperator(
            task_id='has_line_items',
            test="{{ result('get_invoice_items').item_count > 0 }}",
            yes_task='check_invoice_status',
            no_task='skip_processing_no_items'
        )


        skip_processing_no_items = rail.RepliconServiceOperator(
            task_id='skip_processing_no_items',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.skip_processing_no_items_method
            )


        set_no_items_result = rail.WriteLogOperator(
            task_id='set_no_items_result',
            message='No invoice line items available'
        )

        # Check invoice status and mark as billed if needed
        check_invoice_status = rail.IfOperator(
            task_id='check_invoice_status',
            test="{{ result('get_invoice_details').invoice_status == 'urn:replicon:invoice2-status:paid' }}",
            yes_task='mark_as_billed',
            no_task='prepare_invoice_item_uris'
        )

        # Mark invoice as billed if status is Paid
        mark_as_billed = rail.RepliconServiceOperator(
            task_id='mark_as_billed',
            endpoint='/services/InvoiceService2.svc/MarkAsBilled',
            data=lambda dag_run: {
                'invoiceUri': dag_run.conf['invoice_uri']
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        # Prepare invoice item URIs for bulk get
        prepare_invoice_item_uris = rail.PythonOperator(
            task_id='prepare_invoice_item_uris',
            python_callable=lambda: {
                'invoice_item_uris': [item.get('invoiceItemUri') for item in rail.result('get_invoice_items')['invoice_items']]
            }
        )

        # Get detailed information about all invoice items
        get_invoice_item_details = rail.RepliconServiceOperator(
            task_id='get_invoice_item_details',
            endpoint='/services/InvoiceService2.svc/BulkGetInvoiceItem',
            data=lambda: {
                'invoiceItemUris': rail.result('prepare_invoice_item_uris')['invoice_item_uris']
            }
        )

        # Calculate total amount and check for VAT line
        calculate_vat = rail.PythonOperator(
            task_id='calculate_vat',
            python_callable=request_payload.calculate_vat_fun
        )

        # Check if VAT line exists with correct amount
        check_vat_amount = rail.IfOperator(
            task_id='check_vat_amount',
            test="{{ result('calculate_vat').vat_correct }}",
            yes_task='skip_processing_correct_vat',
            no_task='check_remove_vat'
        )


        skip_processing_correct_vat = rail.RepliconServiceOperator(
            task_id='skip_processing_correct_vat',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.skip_processing_correct_vat_method
            )

        set_correct_vat_result = rail.WriteLogOperator(
            task_id='set_correct_vat_result',
            message='VAT is already available with the required amount'
        )

        # Check if existing VAT line needs to be removed
        check_remove_vat = rail.IfOperator(
            task_id='check_remove_vat',
            test="{{ result('calculate_vat').vat_line_uri != None }}",
            yes_task='remove_vat_line',
            no_task='add_vat_line'
        )

        # Remove existing VAT line
        remove_vat_line = rail.RepliconServiceOperator(
            task_id='remove_vat_line',
            endpoint='/services/InvoiceService2.svc/RemoveInvoiceItemFromInvoice',
            data=lambda: {
                'invoiceItemUri': rail.result('calculate_vat')['vat_line_uri'],
                'unitOfWorkId': str(uuid.uuid4())
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        # Add new VAT line with correct amount
        add_vat_line = rail.RepliconServiceOperator(
            task_id='add_vat_line',
            endpoint='/services/InvoiceService2.svc/PutInvoiceItem2',
            data=request_payload.add_vat_line_method,
            data_handler=lambda response: {
                'new_vat_line_uri': response['d']['invoiceItemUri'] if 'd' in response and 'invoiceItemUri' in response['d'] else None
            }
        )

        # Check if invoice was marked as billed and needs to be marked as paid
        check_mark_as_paid = rail.IfOperator(
            task_id='check_mark_as_paid',
            test="{{ get_task_state('mark_as_billed') == 'success' }}",
            yes_task='mark_as_paid',
            no_task='update_invoice_sync_status'
        )

        # Mark invoice as paid if it was marked as billed
        mark_as_paid = rail.RepliconServiceOperator(
            task_id='mark_as_paid',
            endpoint='/services/InvoiceService2.svc/MarkAsPaid',
            data=lambda dag_run: {
                'invoiceUri': dag_run.conf['invoice_uri']
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        update_invoice_sync_status = rail.RepliconServiceOperator(
            task_id='update_invoice_sync_status',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.update_invoice_sync_status_method
            )


        set_processing_result = rail.WriteLogOperator(
            task_id='set_processing_result',
            message='VAT calculation added'
        )


        finish=rail.EmptyOperator(
            task_id='finish',
        )

        # Define task dependencies
        get_invoice_details >> get_invoice_items >> has_line_items
        
        # No line items path
        has_line_items >> rail.Label('No') >> skip_processing_no_items >> set_no_items_result >> finish
        
        # Has line items path
        has_line_items >> rail.Label('Yes') >> check_invoice_status
        
        # Process based on invoice status
        check_invoice_status >> rail.Label('Yes') >> mark_as_billed >> prepare_invoice_item_uris
        check_invoice_status >> rail.Label('No') >> prepare_invoice_item_uris
        
        prepare_invoice_item_uris >> get_invoice_item_details >> calculate_vat >> check_vat_amount
        
        # VAT already correct path
        check_vat_amount >> rail.Label('Yes') >> skip_processing_correct_vat >> set_correct_vat_result >> finish
        
        # VAT needs updating path
        check_vat_amount >> rail.Label('No') >> check_remove_vat
        check_remove_vat >> rail.Label('Yes') >> remove_vat_line >> add_vat_line
        check_remove_vat >> rail.Label('No') >> add_vat_line
        
        # Post-processing tasks
        add_vat_line >> check_mark_as_paid
        check_mark_as_paid >> rail.Label('Yes') >> mark_as_paid >> update_invoice_sync_status
        check_mark_as_paid >> rail.Label('No') >> update_invoice_sync_status
        
        update_invoice_sync_status >> set_processing_result >> finish


    return dag


# Create DAGs for all instances defined in config
rail.for_each_instance(create_dag)