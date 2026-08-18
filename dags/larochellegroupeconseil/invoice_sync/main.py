import rail
import itertools
null = None
from airflow.models import Variable
from larochellegroupeconseil.invoice_sync.utils import python_callable
from larochellegroupeconseil.invoice_sync.utils import request_payload
from datetime import datetime, timedelta, timezone


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'{config.master_dag_id}_{config.instance}',
        description= f'LarochelleGroupeConseil_Invoicesync_master V1.0_{config.instance}',
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

        def page_handler(request, response):
            if len(response['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def get_invoice_detail(row, ignored_data_types):
            cells = row['cells']
            return {
                'invoice': cells[0].get('uri', ''),
                'invoice_number': cells[1].get('textValue', ''),
                'client_uri': cells[2].get('uri', ''),
                'creation_datetime': cells[3].get('textValue', ''),
                'last_modified_datetime': cells[4].get('textValue', ''),
                'invoice_status': {
                    'textValue': cells[5].get('textValue', ''),
                    'uri': cells[5].get('uri', '')
                },
                'payment_due_date': cells[6].get('textValue', ''),
                'invoice_date': cells[7].get('textValue', ''),
                'total_invoice_amount': {k: v for k, v in cells[8].items() if k not in ignored_data_types},
                'invoice_currency': {k: v for k, v in cells[9].items() if k not in ignored_data_types},
                'payment_term': {k: v for k, v in cells[10].items() if k not in ignored_data_types},
                'invoice_amount_in_base_currency': {k: v for k, v in cells[11].items() if k not in ignored_data_types},
                'description': cells[12].get('textValue', ''),
            }
        
        def filter_data(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            accepted_billing_status = ('In Draft',)
            ignored_data_types = ('dataType', 'objectType')
            queued_for_sync_invoice = list(
                map(lambda row: get_invoice_detail(row, ignored_data_types),
                    filter(lambda x: x['cells'][5].get('textValue', '') in accepted_billing_status, flatten_rows))) if flatten_rows else []
            return [invoice for invoice in queued_for_sync_invoice if invoice is not None]
        
        get_queued_for_sync_invoice = rail.RepliconServicePageOperator(
            task_id="get_queued_for_sync_invoice",
            endpoint="/services/InvoiceListService2.svc/GetData",
            data= request_payload.get_queuedforsync_invoice,
            page_handler=page_handler,
            all_result_data_handler=filter_data
        )

        has_list_data = rail.IfOperator(
            task_id='has_list_data',
            test="{{ result('get_queued_for_sync_invoice') | length > 0 }}",
            yes_task='get_required_invoices',
            no_task='empty_task'
        )

        def handle_updated_invoices(response, item):
            def compare_datetime_value(datetime_value):
                datetime_value = datetime(
                    year=datetime_value['year'], month=datetime_value['month'], day=datetime_value['day'],
                    hour=datetime_value['hour'], minute=datetime_value['minute'], second=datetime_value['second'])
                last_synctime = datetime.strptime(
                    rail.result('get_last_sync_time')['last_synctime'], '%Y-%m-%dT%H:%M:%SZ')
                return datetime_value >= last_synctime
            last_modified_timestamp = response['lastModifiedTimestamp']['valueInUtc']
            is_valid_invoice = compare_datetime_value(last_modified_timestamp)
            if is_valid_invoice:
                return response
            return None
        
        get_required_invoices = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_required_invoices",
            items=lambda: [x for x in rail.result(
                'get_queued_for_sync_invoice') if x['invoice']],
            endpoint="/services/InvoiceService2.svc/GetInvoiceDetails",
            data={
                'invoiceUri': '{{ item.invoice }}'
            },
            # flatten=True,
            replicon_conn_id=config.replicon_conn_id,
            data_handler=handle_updated_invoices
        )

        has_invoice_data = rail.IfOperator(
            task_id='has_invoice_data',
            test="{{ result('get_required_invoices') | select | list | length > 0 }}",
            yes_task='json_formatter',
            no_task='empty_task'
        )

        json_formatter = rail.EmptyOperator(
            task_id = 'json_formatter'
        )

        process_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id= 'process_child_dag',
            items= lambda: [inv for inv in  rail.result('get_required_invoices') if inv],
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