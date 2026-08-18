from datetime import timedelta
import re
import rail
from airflow.models import Variable
from rei.invoice_export.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"{config.company_key}_quickbooks_online_invoice_export_child_dag_{config.instance}",
        description=f'QuickBooks Online {config.region} Invoice Export Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_invoice_by_invoicenumber'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_invoice_by_invoicenumber',
            end_task='catch_invoice_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def escape_single_quotes(string):
            return re.sub(r"(')", r"\'", string)
        search_invoice_by_invoicenumber = rail.InternalQuickbooksAPIOperator(
            task_id='search_invoice_by_invoicenumber',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id='{{ dag_run.conf.quickbooks_conn_id }}',
            query_params=lambda dag_run: {
                'query': "SELECT * FROM Invoice WHERE DocNumber = '" + escape_single_quotes(f"{dag_run.conf['invoice_number']}") + "'"
            }
        )

        is_invoice_data = rail.IfOperator(
            task_id='is_invoice_data',
            test=lambda: rail.result('search_invoice_by_invoicenumber')['QueryResponse'] and rail.result(
                'search_invoice_by_invoicenumber')['QueryResponse'].get('Invoice'),
            yes_task='send_invoice_present_exception_mail',
            no_task='search_customer'
        )

        send_invoice_present_exception_mail = rail.EmailOperator(
            task_id='send_invoice_present_exception_mail',
            to=config.tenant_email,
            subject='Replicon Invoice not moved to QBO.',
            html_content="templates/emails/invoice_present.html"
        )

        search_customer = rail.InternalQuickbooksAPIOperator(
            task_id='search_customer',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id='{{ dag_run.conf.quickbooks_conn_id }}',
            query_params=lambda dag_run: {
                # pylint: disable=line-too-long
                'query': "SELECT * FROM Customer WHERE DisplayName = '" + escape_single_quotes(dag_run.conf['client']['textValue']) + "'"
            }
        )

        def parse_qb_customer():
            query_response = rail.result('search_customer')['QueryResponse']
            return query_response['Customer'][0] if query_response and query_response.get('Customer') else ''
        parse_qbo_customer = rail.PythonOperator(
            task_id='parse_qbo_customer',
            python_callable=parse_qb_customer
        )

        is_customer_not_present = rail.IfOperator(
            task_id='is_customer_not_present',
            test="{{ result('parse_qbo_customer') | is_falsy }}",
            yes_task='process_create_customer',
            no_task='search_items_qbo'
        )

        process_create_customer = rail.EmptyOperator(
            task_id='process_create_customer'
        )

        should_create_customer = rail.IfOperator(
            task_id='should_create_customer',
            test="{{ dag_run.conf.customSettings | \
                attr_or_default('createNewCustomerQuickbooks') | is_truthy }}",
            yes_task='get_client_details',
            no_task='send_client_exception_mail'
        )

        get_client_details = rail.RepliconServiceOperator(
            task_id='get_client_details',
            endpoint='/services/ClientService1.svc/GetClientDetails',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "clientUri": "{{ dag_run.conf.client.uri }}"
            }
        )

        create_customer_qbo = rail.InternalQuickbooksAPIOperator(
            task_id='create_customer_qbo',
            request_method='POST',
            endpoint="/customer",
            intuit_conn_id='{{ dag_run.conf.quickbooks_conn_id }}',
            request_body=request_payload.create_customer_qbo_request
        )

        search_items_qbo = rail.InternalQuickbooksAPIOperator(
            task_id='search_items_qbo',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id='{{ dag_run.conf.quickbooks_conn_id }}',
            query_params=lambda: {
                'query': "SELECT * FROM Item WHERE Active"
            }
        )

        get_invoice_items = rail.RepliconServiceOperator(
            task_id="get_invoice_items",
            endpoint="/services/InvoiceService2.svc/GetPageOfInvoiceItemsForInvoice3",
            data=request_payload.get_invoice_item_request,
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        create_variables = rail.PythonOperator(
            task_id='create_variables',
            python_callable=request_payload.prepare_variables
        )

        create_qbo_items_list = rail.PythonOperator(
            task_id='create_qbo_items_list',
            python_callable=request_payload.qbo_items_list
        )

        create_qbo_items_collection = rail.CreateCollectionOperator(
            task_id="create_qbo_items_collection",
            name="items",
            source="{{ result('create_qbo_items_list') | to_json }}"
        )

        query_distinct_items = rail.QueryCollectionOperator(
            task_id='query_distinct_items',
            query="""{{result('create_variables').query_to_use}}""",
        )

        if_item_size_does_not_equal = rail.IfOperator(
            task_id='if_item_size_does_not_equal',
            test="{{ result('query_distinct_items', 'length') != result('create_variables').billing_expense_rate_count }}",
            yes_task='send_service_item_not_exist_mail',
            no_task='look_for_pay_terms_in_qbo'
        )

        send_service_item_not_exist_mail = rail.EmailOperator(
            task_id='send_service_item_not_exist_mail',
            to=config.tenant_email,
            subject='{{ get_company_key() }}',
            html_content="templates/emails/service_item_not_present.html"
        )

        look_for_pay_terms_in_qbo = rail.InternalQuickbooksAPIOperator(
            task_id='look_for_pay_terms_in_qbo',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id='{{ dag_run.conf.quickbooks_conn_id }}',
            query_params=lambda: {
                'query': "select * from Term"
            }
        )

        def billing_type_contains_timesheet(dag_run):
            invoice_items = rail.result('get_invoice_items')
            billing_type = list(map(lambda x: x["billingType"], 
                filter(lambda x: bool(x["billingType"]) and x["billingType"].split(":")[-1] == "timesheet",
                invoice_items)))
            return bool(billing_type)

        if_billing_type_contains_timesheet = rail.IfOperator(
            task_id='if_billing_type_contains_timesheet',
            test=lambda dag_run: billing_type_contains_timesheet(dag_run),
            yes_task='search_taxcodes_qbo',
            no_task='invoice_does_not_contain_timesheet_line_items'
        )

        invoice_does_not_contain_timesheet_line_items = rail.EmptyOperator(
            task_id='invoice_does_not_contain_timesheet_line_items',
        )

        search_taxcodes_qbo = rail.InternalQuickbooksAPIOperator(
            task_id='search_taxcodes_qbo',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id='{{ dag_run.conf.quickbooks_conn_id }}',
            query_params=lambda: {
                'query': "SELECT * FROM TaxCode"
            }
        )

        create_invoice_with_multiline_item = rail.InternalQuickbooksAPIOperator(
            task_id='create_invoice_with_multiline_item',
            request_method='POST',
            endpoint="/invoice",
            intuit_conn_id='{{ dag_run.conf.quickbooks_conn_id }}',
            request_body=request_payload.create_invoice_with_multiline_item_request
        )

        update_invoice_sync_status = rail.RepliconServiceOperator(
            task_id='update_invoice_sync_status',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=request_payload.get_update_invoice_sync_status
        )

        update_invoice_external_system_number = rail.RepliconServiceOperator(
            task_id='update_invoice_external_system_number',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=request_payload.get_update_invoice_external_system_number
        )

        update_invoice_sync_note = rail.RepliconServiceOperator(
            task_id='update_invoice_sync_note',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=request_payload.get_update_invoice_sync_note
        )

        trigger_report_extract_child_dag = rail.TriggerDagRunOperator(
            task_id='trigger_report_extract_child_dag',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"{config.company_key}_quickbooks_online_invoice_export_report_extract_child_dag_{config.instance}",
            conf=request_payload.get_report_extract_child_dag_payload
        )

        wait_for_invoice_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_invoice_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_report_extract_child_dag") }}'
        )

        send_client_exception_mail = rail.EmailOperator(
            task_id='send_client_exception_mail',
            to=config.tenant_email,
            subject='{{ dag_run.conf.company_key }} | Replicon Invoice not moved to QBO. Invoice: {{ dag_run.conf.invoice_number }}',
            html_content="templates/emails/client_exception.html"
        )

        def get_downstreamtasks_error(invoice_number, error_message):
            return {
                'error': f'Error with {invoice_number} - {error_message}'
            }
        catch_invoice_error = rail.PythonOperator(
            task_id='catch_invoice_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.invoice_number }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> catch_invoice_error

        can_run_batch_task >> rail.Label(
            'No') >> search_invoice_by_invoicenumber >> is_invoice_data

        is_invoice_data >> rail.Label(
            'Yes') >> send_invoice_present_exception_mail >> rail.Label(
                'On Error') >> catch_invoice_error
        is_invoice_data >> rail.Label(
            'No') >> search_customer

        search_customer >> parse_qbo_customer >> is_customer_not_present
        is_customer_not_present >> rail.Label(
            'Yes') >> process_create_customer >> should_create_customer
        should_create_customer >> rail.Label(
            'Yes') >> get_client_details >> create_customer_qbo >> search_items_qbo
        should_create_customer >> rail.Label(
            'No') >> send_client_exception_mail >> rail.Label(
                'On Error') >> catch_invoice_error
        is_customer_not_present >> rail.Label(
            'No') >> search_items_qbo

        search_items_qbo >> get_invoice_items >> create_variables >> create_qbo_items_list >> create_qbo_items_collection >> \
        query_distinct_items >> if_item_size_does_not_equal
        if_item_size_does_not_equal >> rail.Label("Yes") >> send_service_item_not_exist_mail >> catch_invoice_error
        if_item_size_does_not_equal >> rail.Label("No") >> look_for_pay_terms_in_qbo
        look_for_pay_terms_in_qbo >> if_billing_type_contains_timesheet
        if_billing_type_contains_timesheet >> rail.Label("Yes") >> search_taxcodes_qbo
        if_billing_type_contains_timesheet >> rail.Label("No") >> invoice_does_not_contain_timesheet_line_items
        search_taxcodes_qbo >> create_invoice_with_multiline_item >> \
        update_invoice_sync_status >> update_invoice_external_system_number >> update_invoice_sync_note

        update_invoice_sync_note >> trigger_report_extract_child_dag >> wait_for_invoice_child_dag >> rail.Label(
            'On Error') >> catch_invoice_error

    return dag


rail.for_each_instance(create_child_dag)
