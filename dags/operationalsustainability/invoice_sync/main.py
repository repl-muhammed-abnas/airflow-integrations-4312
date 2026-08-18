from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable
from operationalsustainability.invoice_sync.utils import python_callable
from operationalsustainability.invoice_sync.utils import request_payload

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description= f'Sync new invoice in Replicon to QuickBooks_{config.instance}',
        company_key=config.company_key,
        schedule_interval=timedelta(minutes=config.master_schedule_interval),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
    ) as dag: 
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_last_sync_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='empty_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_last_sync_time = rail.PythonOperator(
            task_id = 'get_last_sync_time',
            python_callable= lambda: python_callable.last_sync_time(config.last_sync_time_variable)
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
            data= request_payload.get_queuedforsync_invoice,
            page_handler=python_callable.page_handler,
            all_result_data_handler= python_callable.filter_data
        )

        has_list_data = rail.IfOperator(
            task_id='has_list_data',
            test="{{ result('get_queued_for_sync_invoice') | length > 0 }}",
            yes_task='get_required_invoices',
            no_task= 'empty_task'
        )

        get_required_invoices = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_required_invoices",
            items=lambda: [x for x in rail.result(
                'get_queued_for_sync_invoice') if x['invoice']],
            endpoint="/services/InvoiceService2.svc/GetInvoiceDetails",
            data={
                'invoiceUri': '{{ item.invoice }}'
            },
            # flatten=True,
            data_handler=python_callable.handle_updated_invoices
        )

        has_invoice_data = rail.IfOperator(
            task_id='has_invoice_data',
            test="{{ result('get_required_invoices') | select | list | length > 0 }}",
            yes_task='json_formatter',
            no_task='empty_task'
        )

        json_formatter = rail.PythonOperator(
            task_id = 'json_formatter',
            python_callable= python_callable.json_formatter_get_invoice
        )

        process_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id= 'process_child_dag',
            items= "{{result('json_formatter')}}",
            trigger_dag_id= config.child_dag_id,
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

        update_lastsync_time = rail.PythonOperator(
            task_id = 'update_lastsync_time',
            python_callable= lambda: python_callable.update_last_sync(config.last_sync_time_variable)
        )

        empty_task = rail.EmptyOperator(
            task_id = 'empty_task'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> empty_task

        can_run_batch_task >> rail.Label('No') >> get_last_sync_time >> get_sync_status_filter_definition >>\
        get_queued_for_sync_invoice >> has_list_data >> rail.Label('Yes') >> get_required_invoices >> has_invoice_data >> rail.Label('Yes') >>\
        json_formatter >> process_child_dag >> wait_for_child >> update_lastsync_time

        has_invoice_data >> rail.Label('No') >> empty_task

        has_list_data >> rail.Label('No') >> empty_task

rail.for_each_instance(create_main_airflow_dag)