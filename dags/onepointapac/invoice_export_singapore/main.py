from datetime import timedelta
import rail
from airflow.models import Variable
from onepointapac.invoice_export_singapore.utils import custom_methods, request_filters, request_payload


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'OnepointAPAC Singapore Xero Invoice Export Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=config.master_schedule_interval),
        max_active_runs=config.max_active_runs
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
            end_task='finish',
            execution_timeout=timedelta(days=config.execution_timeout_days)
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

        has_updated_invoices = rail.IfOperator(
            task_id='has_updated_invoices',
            test=lambda: len(custom_methods.updated_invoices()) > 0,
            yes_task='dummy_process_invoice',
            no_task='delete_this_dagrun'
        )

        dummy_process_invoice = rail.EmptyOperator(
            task_id='dummy_process_invoice'
        )

        trigger_invoice_child_dag = rail.trigger_parallel_dagrun(
            task_id='trigger_invoice_child_dag',
            items=custom_methods.updated_invoices,
            trigger_dag_id=config.child_dag_id,
            parallel_count=config.parallel_count_invoice_export,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                'currency_code': config.CURRENCY_CODE,
                'xero_conn_id': config.xero_conn_id,
            }
        )

        get_all_invoice_child_dagrun_ids = rail.PythonOperator(
            task_id='get_all_invoice_child_dagrun_ids',
            python_callable=lambda: custom_methods.get_all_invoice_child_dagrun_ids(
                config.parallel_count_invoice_export)
        )

        gather_invoice_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_invoice_error',
            dag_runs="{{ result('get_all_invoice_child_dagrun_ids') }}",
            dagrun_task_id='catch_invoice_error',
            flatten=True
        )

        is_invoice_error = rail.IfOperator(
            task_id='is_invoice_error',
            test="{{ result('gather_invoice_error') | length > 0 }}",
            yes_task='fail_invoice_error',
            no_task='finish'
        )

        fail_invoice_error = rail.FailOperator(
            task_id='fail_invoice_error',
            message="{{ result('gather_invoice_error') | map_to_attr('error') | join('|') }}"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time >> get_sync_status_filter_definition
        get_sync_status_filter_definition >> get_queued_for_sync_invoice >> has_list_data

        has_list_data >> rail.Label(
            'Yes') >> get_required_invoices >> has_updated_invoices
        has_updated_invoices >> rail.Label('Yes') >> dummy_process_invoice >> trigger_invoice_child_dag >> get_all_invoice_child_dagrun_ids >> \
            gather_invoice_error >> is_invoice_error

        is_invoice_error >> rail.Label('Yes') >> fail_invoice_error
        is_invoice_error >> rail.Label('No') >> finish

        has_list_data >> rail.Label("No") >> delete_this_dagrun >> finish
        has_updated_invoices >> rail.Label("No") >> delete_this_dagrun >> finish

    return dag


rail.for_each_instance(create_main_dag)
