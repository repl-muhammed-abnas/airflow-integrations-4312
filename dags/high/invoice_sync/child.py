from datetime import datetime, timedelta, timezone
import rail
from high.invoice_sync.utils import python_callable
from high.invoice_sync.utils import request_payload
from airflow.models import Variable
import uuid


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.child_dag_id,
        description= 'Sync new/updated invoice in Replicon to Xero Add Invoice Child Dag',
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
            no_task='search_invoice_in_xero'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_invoice_in_xero',
            end_task='empty_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        
        search_invoice_in_xero = rail.XeroAPIOperator(
            task_id='search_invoice_in_xero',
            xero_conn_id= config.xero_conn_id,
            endpoint='/api.xro/2.0/Invoices',
            request_method='GET',
            filters='''?where=Reference="{{dag_run.conf.invoiceNumberText}}"'''
        )

        if_invoice_id_present_and_status_not_voided_deleted = rail.IfOperator(
            task_id='if_invoice_id_present_and_status_not_voided_deleted',
            test=lambda: rail.result('search_invoice_in_xero') and rail.result('search_invoice_in_xero')['Invoices'] and 
                    rail.result('search_invoice_in_xero')['Invoices'][0]['InvoiceID'] and (rail.result(
                    'search_invoice_in_xero')['Invoices'][0]['Status'] not in ["DELETED", "VOIDED"]),
            yes_task="send_email_invoice_present",
            no_task="get_client_details_in_replicon",
        )

        send_email_invoice_present = rail.EmailOperator(
            task_id='send_email_invoice_present',
            to= config.notification_email,
            subject='Invoice present. Replicon Invoice not moved to Xero. Invoice: "{{dag_run.conf.invoiceNumberText}}"',
            html_content="templates/invoice_present_mail.html",
        )

        get_client_details_in_replicon = rail.RepliconServiceOperator(
            task_id='get_client_details_in_replicon',
            endpoint='/services/ClientService1.svc/GetClientDetails',
            replicon_conn_id= config.replicon_conn_id,
            data={
                "clientUri": "{{dag_run.conf.client.uri}}"
            }
        )

        search_contact_in_xero = rail.XeroAPIOperator(
            task_id='search_contact_in_xero',
            xero_conn_id= config.xero_conn_id,
            endpoint='/api.xro/2.0/Contacts',
            request_method='GET',
            filters='?where=Name="{{dag_run.conf.client.name}}"'
        )

        if_contact_not_present_in_xero = rail.IfOperator(
            task_id='if_contact_not_present_in_xero',
            test=lambda: not (rail.result('search_contact_in_xero') and rail.result('search_contact_in_xero')['Contacts'] and rail.result(
                'search_contact_in_xero')['Contacts'][0].get('ContactID')),
            yes_task="if_create_contact_in_xero_is_true",
            no_task="get_all_invoice_items",
        )

        if_create_contact_in_xero_is_true = rail.IfOperator(
            task_id='if_create_contact_in_xero_is_true',
            test=lambda: bool(config.create_contact_in_xero_if_missing),
            yes_task="create_contact_in_xero",
            no_task="if_create_contact_in_xero_is_false",
        )

        create_contact_in_xero = rail.XeroAPIOperator(
            task_id='create_contact_in_xero',
            xero_conn_id= config.xero_conn_id,
            endpoint='/api.xro/2.0/Contacts',
            request_method='POST',
            request_body=request_payload.get_create_contact_payload
        )

        if_create_contact_in_xero_is_false = rail.IfOperator(
            task_id='if_create_contact_in_xero_is_false',
            test=lambda: bool(config.create_contact_in_xero_if_missing),
            yes_task="get_all_invoice_items",
            no_task="send_mail_invoice_not_created_as_contact_not_present",
        )

        send_mail_invoice_not_created_as_contact_not_present = rail.EmailOperator(
            task_id='send_mail_invoice_not_created_as_contact_not_present',
            to= config.notification_email,
            subject='Replicon Invoice not moved to Xero. Invoice: "{{dag_run.conf.invoiceNumberText}}"',
            html_content="templates/invoice_not_created_mail.html",
        )

        get_all_invoice_items = rail.RepliconServiceOperator(
            task_id='get_all_invoice_items',
            endpoint="/services/InvoiceService2.svc/GetPageOfInvoiceItemsForInvoice3",
            data=request_payload.get_invoice_item_request,
            replicon_conn_id= config.replicon_conn_id
        )

        get_invoice_creation_payload = rail.PythonOperator(
            task_id='get_invoice_creation_payload',
            python_callable=request_payload.get_invoice_payload_with_line_items,
        )

        if_invoice_has_line_items_tobe_added = rail.IfOperator(
            task_id='if_invoice_has_line_items_tobe_added',
            test=lambda: bool(rail.result('get_invoice_creation_payload')),
            yes_task='create_invoice_in_xero',
            no_task='update_invoice_sync_status'
        )

        create_invoice_in_xero = rail.XeroAPIOperator(
            task_id='create_invoice_in_xero',
            xero_conn_id= config.xero_conn_id,
            endpoint='/api.xro/2.0/Invoices',
            request_method='POST',
            request_body=lambda: rail.result('get_invoice_creation_payload')
        )

        update_invoice_sync_status = rail.RepliconServiceOperator(
            task_id='update_invoice_sync_status',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            replicon_conn_id= config.replicon_conn_id,
            data=request_payload.get_update_invoice_sync_status
        )

        update_invoice_external_system_number = rail.RepliconServiceOperator(
            task_id='update_invoice_external_system_number',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            replicon_conn_id= config.replicon_conn_id,
            data=request_payload.get_update_invoice_external_system_number
        )

        update_invoice_sync_note = rail.RepliconServiceOperator(
            task_id='update_invoice_sync_note',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            replicon_conn_id= config.replicon_conn_id,
            data=request_payload.get_update_invoice_sync_note
        )

        empty_task = rail.EmptyOperator(
            task_id = 'empty_task'
        )



        can_run_batch_task >> rail.Label('Yes') >> batch_task >> empty_task

        can_run_batch_task >> rail.Label('No') >> search_invoice_in_xero

        search_invoice_in_xero >> if_invoice_id_present_and_status_not_voided_deleted >> rail.Label("Yes") >>\
        send_email_invoice_present >> empty_task

        if_invoice_id_present_and_status_not_voided_deleted >> rail.Label("No") >> get_client_details_in_replicon >>\
        search_contact_in_xero >> if_contact_not_present_in_xero >> rail.Label("Yes") >> if_create_contact_in_xero_is_true>>\
        rail.Label("Yes") >> create_contact_in_xero

        if_create_contact_in_xero_is_true>> rail.Label("No") >> if_create_contact_in_xero_is_false >> rail.Label("Yes") >>\
        send_mail_invoice_not_created_as_contact_not_present

        if_create_contact_in_xero_is_false >> rail.Label("No") >> get_all_invoice_items

        if_contact_not_present_in_xero >> rail.Label("No") >> get_all_invoice_items

        create_contact_in_xero >> get_all_invoice_items >> get_invoice_creation_payload >> if_invoice_has_line_items_tobe_added >> rail.Label("Yes") >>\
        create_invoice_in_xero >> update_invoice_sync_status

        if_invoice_has_line_items_tobe_added >> rail.Label("No") >> update_invoice_sync_status

        update_invoice_sync_status >> update_invoice_external_system_number >> update_invoice_sync_note





rail.for_each_instance(create_child_dag)