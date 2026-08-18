"""MMR Consulting Replicon -> Xero invoice export master DAG.

Fetches invoices queued for sync and routes each to the matching country child DAG
based on the client's billing country.
"""
from datetime import timedelta
import rail
from airflow.models import Variable
from mmr_consulting.invoice_export.utils import custom_methods, request_filters, request_payload


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"mmr_consulting_invoice_export_master_{config.instance}",
        description=f'MMR Consulting Xero Invoice Export Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_lastsync_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_lastsync_time',
            end_task='update_lastsync_time',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_lastsync_time = rail.PythonOperator(
            task_id='get_lastsync_time',
            python_callable=lambda: custom_methods.read_lastsync_time(config)
        )

        get_sync_status_filter_definition = rail.RepliconServiceOperator(
            task_id='get_sync_status_filter_definition',
            endpoint='/services/InvoiceListService2.svc/GetAllFilterDefinitions',
            data_handler=lambda response: next(
                iter(filter(lambda x: x['name'] == 'Sync Status', response)), {}).get('uri', '')
        )

        get_queued_for_sync_invoice = rail.RepliconServicePageOperator(
            task_id="get_queued_for_sync_invoice",
            endpoint="/services/InvoiceListService2.svc/GetData",
            data=request_payload.get_queuedforsync_invoice,
            page_handler=request_filters.page_handler,
            all_result_data_handler=request_filters.filter_data
        )

        has_list_data = rail.IfOperator(
            task_id='has_list_data',
            test="{{ result('get_queued_for_sync_invoice') | length > 0 }}",
            yes_task='get_required_invoices',
            no_task='delete_this_dagrun'
        )

        get_required_invoices = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_required_invoices",
            items=lambda: [x for x in rail.result(
                'get_queued_for_sync_invoice') if x['invoice']],
            endpoint="/services/InvoiceService2.svc/GetInvoiceDetails",
            data={
                'invoiceUri': '{{ item.invoice }}'
            },
            flatten=True,
            data_handler=request_filters.handle_updated_invoices
        )

        enrich_invoices_with_client_country = rail.RepliconServiceCallForEachItemOperator(
            task_id="enrich_invoices_with_client_country",
            items=lambda: [x for x in rail.result('get_required_invoices') if x],
            endpoint="/services/ClientService1.svc/GetClientDetails",
            data={
                'clientUri': '{{ item.client.uri }}'
            },
            flatten=False,
            data_handler=request_filters.attach_client_country
        )

        has_xero_invoice_data = rail.IfOperator(
            task_id='has_xero_invoice_data',
            test=lambda: len([x for x in rail.result('enrich_invoices_with_client_country')
                              if x and custom_methods.country_for_item(x)]) > 0,
            yes_task='trigger_invoice_child_dag',
            no_task='delete_this_dagrun'
        )

        trigger_invoice_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_invoice_child_dag',
            thread_pool_size=4,
            retries=0,
            items=custom_methods.routable_invoices,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=lambda item: (
                f"mmr_consulting_invoice_export_{custom_methods.country_for_item(item)}_child_dag_{config.instance}"),
            conf=lambda item: {
                **dict(item.items()),
                'country': custom_methods.country_for_item(item),
                'currency_code': custom_methods.currency_code_for_item(item),
                'xero_conn_id': getattr(config, custom_methods.country_for_item(item)),
            }
        )

        wait_for_invoice_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_invoice_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_invoice_child_dag") }}'
        )

        gather_invoice_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_invoice_error',
            dag_runs="{{ result('trigger_invoice_child_dag') }}",
            dagrun_task_id='catch_invoice_error',
            flatten=True
        )

        is_invoice_error = rail.IfOperator(
            task_id='is_invoice_error',
            test="{{ result('gather_invoice_error') | length > 0 }}",
            yes_task='fail_invoice_error',
            no_task='update_lastsync_time'
        )

        fail_invoice_error = rail.FailOperator(
            task_id='fail_invoice_error',
            message="{{ result('gather_invoice_error') | map_to_attr('error') | join('|') }}"
        )


        update_lastsync_time = rail.PythonOperator(
            task_id='update_lastsync_time',
            python_callable=lambda: custom_methods.write_lastsync_time(config)
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> update_lastsync_time
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time >> get_sync_status_filter_definition
        get_sync_status_filter_definition >> get_queued_for_sync_invoice >> has_list_data

        has_list_data >> rail.Label(
            'Yes') >> get_required_invoices >> enrich_invoices_with_client_country >> has_xero_invoice_data
        has_xero_invoice_data >> rail.Label(
            "Yes") >> trigger_invoice_child_dag >> wait_for_invoice_child_dag >> \
            gather_invoice_error >> is_invoice_error

        is_invoice_error >> rail.Label(
            'Yes') >> fail_invoice_error 
        is_invoice_error >> rail.Label(
            'No') >> update_lastsync_time
        
        has_list_data >> rail.Label("No") >> delete_this_dagrun
        has_xero_invoice_data >> rail.Label("No") >> delete_this_dagrun
       

    return dag


rail.for_each_instance(create_main_dag)
