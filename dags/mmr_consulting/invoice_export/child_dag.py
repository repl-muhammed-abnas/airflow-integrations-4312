"""Per-invoice child DAG builder for the MMR Consulting invoice export.

One child DAG per country; all share this logic and differ only by Xero organisation.
"""
from datetime import timedelta
from airflow.models import Variable
import rail
from mmr_consulting.invoice_export.utils import custom_methods, request_filters, request_payload
from mmr_consulting.invoice_export.mapper import countries, xero_mappings


def create_child_dag(config, country):
    with rail.create_airflow_dag(
        dag_id=f"mmr_consulting_invoice_export_{country}_child_dag_{config.instance}",
        description=f'MMR Consulting Xero {country} Invoice Export Child Dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_invoice_export_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_invoice_status_invoiced'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_invoice_status_invoiced',
            end_task='catch_invoice_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Only Invoiced status is synced; Billed/Paid end gracefully with no error.
        is_invoice_status_invoiced = rail.IfOperator(
            task_id='is_invoice_status_invoiced',
            test=lambda dag_run: (dag_run.conf.get('invoice_status') or {}).get(
                'textValue') == config.REQUIRED_INVOICE_STATUS,
            yes_task='search_invoice_in_xero',
            no_task='catch_invoice_error'
        )

        # Dedup guard: prevents creating a duplicate invoice on re-run.
        search_invoice_in_xero = rail.XeroAPIOperator(
            task_id='search_invoice_in_xero',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Invoices',
            request_method='GET',
            filters='''?where=InvoiceNumber="{{dag_run.conf.invoice_number}}"'''
        )

        if_invoice_id_present_and_status_not_voided_deleted = rail.IfOperator(
            task_id='if_invoice_id_present_and_status_not_voided_deleted',
            test=lambda: rail.result('search_invoice_in_xero') and rail.result('search_invoice_in_xero')['Invoices'] and rail.result(
                'search_invoice_in_xero')['Invoices'][0]['InvoiceID'] and (rail.result(
                    'search_invoice_in_xero')['Invoices'][0]['Status'] not in ["DELETED", "VOIDED"]),
            yes_task="send_email_invoice_present",
            no_task="get_client_details_in_replicon",
        )

        send_email_invoice_present = rail.EmailOperator(
            task_id='send_email_invoice_present',
            to=config.internal_logs_email,
            subject='Replicon Invoice "{{dag_run.conf.invoice_number}}" not synced to Xero',
            html_content="templates/invoice_present_mail.html",
        )

        get_client_details_in_replicon = rail.RepliconServiceOperator(
            task_id='get_client_details_in_replicon',
            endpoint='/services/ClientService1.svc/GetClientDetails',
            data={
                "clientUri": "{{dag_run.conf.client.uri}}"
            }
        )

        search_contact_in_xero = rail.XeroAPIOperator(
            task_id='search_contact_in_xero',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Contacts',
            request_method='GET',
            filters='?where=Name="{{dag_run.conf.client.textValue}}"'
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
            test=lambda : config.createNewContactXero,
            yes_task="create_contact_in_xero",
            no_task="if_create_contact_in_xero_is_false",
        )

        create_contact_in_xero = rail.XeroAPIOperator(
            task_id='create_contact_in_xero',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Contacts',
            request_method='POST',
            request_body=request_payload.get_create_contact_payload
        )

        if_create_contact_in_xero_is_false = rail.IfOperator(
            task_id='if_create_contact_in_xero_is_false',
            test=lambda : not (config.createNewContactXero),
            yes_task="send_mail_invoice_not_created_as_contact_not_present",
            no_task="get_all_invoice_items",
        )

        send_mail_invoice_not_created_as_contact_not_present = rail.EmailOperator(
            task_id='send_mail_invoice_not_created_as_contact_not_present',
            to=config.internal_logs_email,
            subject='Replicon Invoice "{{dag_run.conf.invoice_number}}" not synced to Xero',
            html_content="templates/invoice_not_created_mail.html",
        )

        get_all_invoice_items = rail.RepliconServiceOperator(
            task_id='get_all_invoice_items',
            endpoint="/services/InvoiceService2.svc/GetPageOfInvoiceItemsForInvoice3",
            data=request_payload.get_invoice_item_request,
        )

        # Fetches item-level description for fixed-bid and adhoc lines.
        enrich_invoice_items = rail.RepliconServiceCallForEachItemOperator(
            task_id='enrich_invoice_items',
            items=lambda: [item for item in (rail.result('get_all_invoice_items') or [])
                           if item.get('invoiceItemUri')],
            endpoint="/services/InvoiceService2.svc/GetInvoiceItem",
            data={
                'invoiceItemUri': '{{ item.invoiceItemUri }}'
            },
            flatten=False,
            data_handler=lambda response, item: {
                'invoiceItemUri': item.get('invoiceItemUri'),
                'description': request_filters.extract_invoice_item_description(response)
            }
        )

        # Fetches the PO Type extension field per project to branch timesheet and adhoc lines.
        get_project_po_types = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_project_po_types',
            items=custom_methods.distinct_project_uris,
            endpoint="/services/ProjectService1.svc/GetProjectDetails2",
            data={
                'projectUri': '{{ item }}'
            },
            flatten=False,
            data_handler=lambda response, item: {
                'uri': item,
                'name': response.get('name'),
                'po_type': request_filters.extract_project_po_type(response)
            }
        )

        get_invoice_creation_payload = rail.PythonOperator(
            task_id='get_invoice_creation_payload',
            python_callable=request_payload.get_invoice_payload_with_line_items,
        )

        if_invoice_has_line_items_tobe_added = rail.IfOperator(
            task_id='if_invoice_has_line_items_tobe_added',
            test=lambda: bool(rail.result('get_invoice_creation_payload')),
            yes_task='create_invoice_in_xero',
            no_task='no_invoice_created'
        )

        no_invoice_created = rail.PythonOperator(
            task_id='no_invoice_created',
            python_callable=lambda: "No invoice created - no line items to add for invoice {{ dag_run.conf.invoice_number }}",
        )

        create_invoice_in_xero = rail.XeroAPIOperator(
            task_id='create_invoice_in_xero',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Invoices',
            request_method='POST',
            request_body=lambda: rail.result('get_invoice_creation_payload')
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

        catch_invoice_error = rail.PythonOperator(
            task_id='catch_invoice_error',
            trigger_rule='one_failed',
            python_callable=custom_methods.get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.invoice_number }}-{{ dag_run.conf.client.textValue }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> rail.Label(
            'On Error') >> catch_invoice_error
        can_run_batch_task >> rail.Label(
            'No') >> is_invoice_status_invoiced
        is_invoice_status_invoiced >> rail.Label(
            'Yes') >> search_invoice_in_xero >> if_invoice_id_present_and_status_not_voided_deleted
        is_invoice_status_invoiced >> rail.Label(
            'No') >> catch_invoice_error
        if_invoice_id_present_and_status_not_voided_deleted >> rail.Label(
            'Yes') >> send_email_invoice_present >> catch_invoice_error
        if_invoice_id_present_and_status_not_voided_deleted >> rail.Label(
            'No') >> get_client_details_in_replicon >> search_contact_in_xero
        search_contact_in_xero >> if_contact_not_present_in_xero
        if_contact_not_present_in_xero >> rail.Label(
            'Yes') >> if_create_contact_in_xero_is_true
        if_create_contact_in_xero_is_true >> rail.Label(
            'Yes') >> create_contact_in_xero >> if_create_contact_in_xero_is_false
        if_create_contact_in_xero_is_true >> rail.Label(
            'No') >> if_create_contact_in_xero_is_false
        if_create_contact_in_xero_is_false >> rail.Label(
            'Yes') >> send_mail_invoice_not_created_as_contact_not_present >> catch_invoice_error
        if_create_contact_in_xero_is_false >> rail.Label(
            'No') >> get_all_invoice_items
        if_contact_not_present_in_xero >> rail.Label(
            'No') >> get_all_invoice_items
        get_all_invoice_items >> enrich_invoice_items >> get_project_po_types >> \
            get_invoice_creation_payload >> if_invoice_has_line_items_tobe_added
        if_invoice_has_line_items_tobe_added >> rail.Label(
            'Yes') >> create_invoice_in_xero >> update_invoice_sync_status
        if_invoice_has_line_items_tobe_added >> rail.Label(
            'No') >> no_invoice_created
        update_invoice_sync_status >> update_invoice_external_system_number
        update_invoice_external_system_number >> update_invoice_sync_note >> rail.Label(
            'On Error') >> catch_invoice_error

    return dag


def create_child_dags(config):
    return [create_child_dag(config, country) for country in countries.COUNTRIES]


rail.for_each_instance(create_child_dags)
