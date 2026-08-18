import rail
import itertools
from operationalsustainability.invoice_sync.utils import python_callable
from operationalsustainability.invoice_sync.utils import request_payload
from datetime import datetime, timedelta, timezone
import re
from airflow.models import Variable
import uuid

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.child_dag_id,
        description= 'Sync new invoice in Replicon to QuickBooks Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='invoice_items'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='invoice_items',
            end_task='empty_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        invoice_items = rail.RepliconServiceOperator(
            task_id= 'invoice_items',
            endpoint="/services/InvoiceService2.svc/GetPageOfInvoiceItemsForInvoice3",
            data= {
                "page": 1,
                "pageSize": 10000,
                "invoice": {
                    "uri": "{{ dag_run.conf.uri }}"    #invoice uri
                },
                "invoiceItemColumnOptions": [
                "urn:replicon:invoice-item-column-option:project",
                ]}
        )

        json_formatter = rail.PythonOperator(
            task_id = 'json_formatter',
            python_callable= python_callable.json_formatter_invoice_items
        )

        process_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id= 'process_child_dag',
            items= "{{result('json_formatter')}}",
            trigger_dag_id= config.invoice_items_loop_dag_id,
            retries=0,
            conf=lambda item: {
                **dict(item.items())
            }
        )

        wait_for_child = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_child_dag") }}'
        )

        gather_invoice_list = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_invoice_error',
            dag_runs="{{ result('process_child_dag') }}",
            dagrun_task_id='append_to_invoice_data_expense',
            flatten=True
        )

        search_invoice_qbo = rail.InternalQuickbooksAPIOperator(
            task_id='search_invoice_qbo',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id= config.qbo_conn_id,
            query_params=lambda dag_run: {
                'query': f"SELECT * FROM Invoice WHERE DocNumber = 'REP-{python_callable.escape_sql_string(dag_run.conf.get('invoiceNumberText', ''))}'"
            }
        )

        is_invoice_id_present = rail.IfOperator(
            task_id='is_invoice_id_present',
            test= lambda : bool((invoices := rail.result('search_invoice_qbo').get('QueryResponse', {}).get('Invoice', [])) \
                                and len(invoices) > 0 \
                                and invoices[0].get('Id')),
            yes_task='send_email_invoice_present',
            no_task='search_customer_qbo'
        )

        send_email_invoice_present = rail.EmailOperator(
            task_id='send_email_invoice_present',
            to = config.notification_email,
            subject= 'Replicon Invoice not moved to QBO. Invoice: REP-{{ dag_run.conf.invoiceNumberText }} exists',
            html_content = 'templates/invoice_present_email.html'
        )

        search_customer_qbo = rail.InternalQuickbooksAPIOperator(
            task_id='search_customer_qbo',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id= config.qbo_conn_id,
            query_params=lambda dag_run: {
                # pylint: disable=line-too-long
                'query': f"SELECT * FROM Customer WHERE DisplayName = '{python_callable.escape_sql_string(dag_run.conf.get('client', {}).get('name', ''))}'"
            }
        )

        parse_qbo_customer = rail.PythonOperator(
            task_id='parse_qbo_customer',
            python_callable= python_callable.parse_qb_customer
        )

        is_display_name_not_present = rail.IfOperator(
            task_id = 'is_display_name_not_present',
            test = "{{ result('parse_qbo_customer').get('DisplayName', '') | falsy }}",
            yes_task = 'is_create_customer_in_quickbooks_if_missing_true',
            no_task= 'create_invoice_qbo'
        )

        is_create_customer_in_quickbooks_if_missing_true = rail.IfOperator(
            task_id = 'is_create_customer_in_quickbooks_if_missing_true',
            test = config.create_customer_in_quickbooks_if_missing,
            yes_task = 'get_client_details',
            no_task= 'is_create_customer_in_quickbooks_if_missing_false'
        )

        get_client_details = rail.RepliconServiceOperator(
            task_id='get_client_details',
            endpoint="/services/ClientService1.svc/BulkGetClientDetails3",
            data=lambda dag_run: {
              "clients": [
                {
                  "uri": dag_run.conf['client']['uri']
                }
              ]
          }
        )

        create_customer_qbo = rail.InternalQuickbooksAPIOperator(
            task_id='create_customer_qbo',
            request_method='POST',
            endpoint="/customer",
            intuit_conn_id= config.qbo_conn_id,
            request_body=request_payload.create_customer_qbo_request
        )

        is_create_customer_in_quickbooks_if_missing_false = rail.IfOperator(
            task_id = 'is_create_customer_in_quickbooks_if_missing_false',
            test = config.create_customer_in_quickbooks_if_missing,
            yes_task = 'send_email_client_not_present',
            no_task= 'empty_task'
        )

        send_email_client_not_present = rail.EmailOperator(
            task_id='send_email_client_not_present',
            to = config.notification_email,
            subject= 'Replicon Invoice not moved to QBO. Invoice: {{ dag_run.conf.invoiceNumberText }}',
            html_content = 'templates/client_not_present_email.html'
        )

        empty_task = rail.EmptyOperator(
            task_id= 'empty_task'
        )

        create_invoice_qbo = rail.InternalQuickbooksAPIOperator(
            task_id='create_invoice_qbo',
            request_method='POST',
            endpoint="/invoice",
            intuit_conn_id= config.qbo_conn_id,
            request_body= lambda dag_run: request_payload.create_invoice_with_multiline_item_request(dag_run, rail.result('gather_invoice_list'))
        )

        update_invoice_sync_status = rail.RepliconServiceOperator(
            task_id='update_invoice_sync_status',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_invoice_sync_status
        )

        update_invoice_external_system_number = rail.RepliconServiceOperator(
            task_id='update_invoice_external_system_number',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_invoice_external_system_number
        )

        update_invoice_sync_note = rail.RepliconServiceOperator(
            task_id='update_invoice_sync_note',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_invoice_sync_note
        )




        can_run_batch_task >> rail.Label('Yes') >> batch_task >> empty_task

        can_run_batch_task >> rail.Label('No') >> invoice_items
        
        invoice_items >> json_formatter >> process_child_dag\
            >> wait_for_child >> gather_invoice_list >> search_invoice_qbo >> is_invoice_id_present >>\
        rail.Label("Yes") >> send_email_invoice_present

        is_invoice_id_present >> rail.Label("No") >> search_customer_qbo >> parse_qbo_customer >> is_display_name_not_present >> rail.Label("Yes") >>\
        is_create_customer_in_quickbooks_if_missing_true >> rail.Label("Yes") >> get_client_details >> create_customer_qbo

        is_create_customer_in_quickbooks_if_missing_true >> rail.Label("No") >> is_create_customer_in_quickbooks_if_missing_false >>\
        rail.Label("Yes") >> send_email_client_not_present

        is_create_customer_in_quickbooks_if_missing_false >> rail.Label("No") >> empty_task

        is_display_name_not_present >> rail.Label("No") >> create_invoice_qbo >> update_invoice_sync_status >>\
        update_invoice_external_system_number >> update_invoice_sync_note



rail.for_each_instance(create_child_dag)