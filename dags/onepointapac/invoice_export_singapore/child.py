from datetime import timedelta
from airflow.models import Variable
import rail
from onepointapac.invoice_export_singapore.utils import custom_methods, request_payload, request_filters


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f'OnepointAPAC Singapore Xero Invoice Export Child {config.instance}',
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
            no_task='is_currency_sgd'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_currency_sgd',
            end_task='catch_invoice_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Recipe guard: only Singapore invoices are processed; others stop without error.
        is_currency_sgd = rail.IfOperator(
            task_id='is_currency_sgd',
            test=custom_methods.is_currency_sgd,
            yes_task='is_invoice_status_processable',
            no_task='skip_invoice'
        )

        # Recipe guard: invoices already Billed or Paid stop without error.
        is_invoice_status_processable = rail.IfOperator(
            task_id='is_invoice_status_processable',
            test=custom_methods.is_invoice_status_processable,
            yes_task='search_invoice_in_xero',
            no_task='skip_invoice'
        )

        # Dedup guard: match the exact Reference we write ("Proforma Invoice #<number>").
        # Exact equality (not Contains) avoids substring collisions where #42 would also
        # match #420/#142. XeroAPIOperator URL-encodes the where-clause, so the '#' and
        # spaces in the reference are handled safely.
        search_invoice_in_xero = rail.XeroAPIOperator(
            task_id='search_invoice_in_xero',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Invoices',
            request_method='GET',
            filters='?where=Reference=="' + config.REFERENCE_PREFIX + '{{ dag_run.conf.invoice_number }}"'
        )

        if_invoice_present_and_not_voided_deleted = rail.IfOperator(
            task_id='if_invoice_present_and_not_voided_deleted',
            test=lambda: rail.result('search_invoice_in_xero') and rail.result('search_invoice_in_xero')['Invoices'] and rail.result(
                'search_invoice_in_xero')['Invoices'][0]['InvoiceID'] and (rail.result(
                    'search_invoice_in_xero')['Invoices'][0]['Status'] not in ["DELETED", "VOIDED"]),
            yes_task="send_email_invoice_present",
            no_task="get_client_details_in_replicon",
        )

        send_email_invoice_present = rail.EmailOperator(
            task_id='send_email_invoice_present',
            to=config.internal_logs_email,
            subject='Invoice present. Replicon Invoice "{{ dag_run.conf.invoice_number }}" not moved to Xero',
            html_content="templates/invoice_present_mail.html",
        )

        get_client_details_in_replicon = rail.RepliconServiceOperator(
            task_id='get_client_details_in_replicon',
            endpoint='/services/ClientService1.svc/GetClientDetails',
            data={
                "clientUri": "{{ dag_run.conf.client.uri }}"
            }
        )

        search_contact_in_xero = rail.XeroAPIOperator(
            task_id='search_contact_in_xero',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Contacts',
            request_method='GET',
            filters='?where=Name="{{ dag_run.conf.client.textValue }}"'
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
            test=lambda: config.createNewContactXero,
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
            test=lambda: not config.createNewContactXero,
            yes_task="send_mail_invoice_not_created_as_contact_not_present",
            no_task="get_all_invoice_items",
        )

        send_mail_invoice_not_created_as_contact_not_present = rail.EmailOperator(
            task_id='send_mail_invoice_not_created_as_contact_not_present',
            to=config.internal_logs_email,
            subject='Replicon Invoice "{{ dag_run.conf.invoice_number }}" not moved to Xero',
            html_content="templates/invoice_not_created_mail.html",
        )

        get_all_invoice_items = rail.RepliconServiceOperator(
            task_id='get_all_invoice_items',
            endpoint="/services/InvoiceService2.svc/GetPageOfInvoiceItemsForInvoice3",
            data=request_payload.get_invoice_item_request,
        )

        # Ad-hoc lines take their description from item-level customMetadata.
        enrich_invoice_items = rail.RepliconServiceCallForEachItemOperator(
            task_id='enrich_invoice_items',
            items=lambda: [item for item in (rail.result('get_all_invoice_items') or [])
                           if item.get('invoiceItemUri') and 'adhoc' in (item.get('billingType') or '')],
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
            python_callable=lambda: "No invoice created - no line items for invoice {{ dag_run.conf.invoice_number }}",
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

        # Graceful skip terminal for the recipe's stop_with_error=false paths (non-SGD,
        # Billed/Paid, duplicate-in-Xero, contact-missing-and-not-created). one_success so it
        # runs for whichever single skip branch was taken.
        skip_invoice = rail.PythonOperator(
            task_id='skip_invoice',
            trigger_rule='one_success',
            python_callable=lambda: "Invoice skipped - {{ dag_run.conf.invoice_number }}",
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
        can_run_batch_task >> rail.Label('No') >> is_currency_sgd

        is_currency_sgd >> rail.Label('Yes') >> is_invoice_status_processable
        is_currency_sgd >> rail.Label('No') >> skip_invoice
        is_invoice_status_processable >> rail.Label('Yes') >> search_invoice_in_xero
        is_invoice_status_processable >> rail.Label('No') >> skip_invoice

        search_invoice_in_xero >> if_invoice_present_and_not_voided_deleted
        if_invoice_present_and_not_voided_deleted >> rail.Label(
            'Yes') >> send_email_invoice_present >> skip_invoice
        if_invoice_present_and_not_voided_deleted >> rail.Label(
            'No') >> get_client_details_in_replicon >> search_contact_in_xero

        search_contact_in_xero >> if_contact_not_present_in_xero
        if_contact_not_present_in_xero >> rail.Label(
            'Yes') >> if_create_contact_in_xero_is_true
        if_create_contact_in_xero_is_true >> rail.Label(
            'Yes') >> create_contact_in_xero >> if_create_contact_in_xero_is_false
        if_create_contact_in_xero_is_true >> rail.Label(
            'No') >> if_create_contact_in_xero_is_false
        if_create_contact_in_xero_is_false >> rail.Label(
            'Yes') >> send_mail_invoice_not_created_as_contact_not_present >> skip_invoice
        if_create_contact_in_xero_is_false >> rail.Label(
            'No') >> get_all_invoice_items
        if_contact_not_present_in_xero >> rail.Label(
            'No') >> get_all_invoice_items

        get_all_invoice_items >> enrich_invoice_items >> \
            get_invoice_creation_payload >> if_invoice_has_line_items_tobe_added
        if_invoice_has_line_items_tobe_added >> rail.Label(
            'Yes') >> create_invoice_in_xero >> update_invoice_sync_status
        if_invoice_has_line_items_tobe_added >> rail.Label(
            'No') >> no_invoice_created
        update_invoice_sync_status >> update_invoice_external_system_number
        update_invoice_external_system_number >> update_invoice_sync_note >> rail.Label(
            'On Error') >> catch_invoice_error

    return dag


rail.for_each_instance(create_child_dag)
