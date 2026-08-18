from datetime import datetime, timedelta, timezone
import itertools
import rail
from airflow.models import Variable
from operationalsustainability.client_sync.utils import python_callable
from operationalsustainability.client_sync.utils import request_payload
null = None


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description= f'Sync new client in Replicon to QuickBooks_{config.instance}',
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
        
        get_clients = rail.RepliconServicePageOperator(
            task_id="get_clients",
            endpoint="/services/ClientListService1.svc/GetData",
            page_handler= python_callable.page_handler,
            all_result_data_handler= python_callable.filter_data,
            data = {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:client-list-column:active",
                    "urn:replicon:client-list-column:client"
                ],
                "sort": [],
                "filterExpression": null
                }
        )

        has_list_data = rail.IfOperator(
            task_id = 'has_list_data',
            test= "{{ result('get_clients') | length > 0 }}",
            yes_task='formatted_client_names',
            no_task= 'empty_task'
        )

        formatted_client_names = rail.PythonOperator(
            task_id='formatted_client_names',
            python_callable= python_callable.build_customer_search_query
        )
        
        search_customer = rail.InternalQuickbooksAPIOperator(
            task_id='search_customer',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id= config.qbo_conn_id,
            query_params=lambda dag_run: {
                # pylint: disable=line-too-long
                'query': "SELECT * FROM Customer WHERE DisplayName in (" + rail.result('formatted_client_names') + ")"
            }
        )
        
        parse_qbo_customer = rail.PythonOperator(
            task_id='parse_qbo_customer',
            python_callable= python_callable.parse_qb_customer
        )

        is_customer_not_present = rail.IfOperator(
            task_id='is_customer_not_present',
            test="{{ result('parse_qbo_customer') | is_falsy }}",
            yes_task='json_formatter',
            no_task='empty_task'
        )

        json_formatter = rail.PythonOperator(
            task_id = 'json_formatter',
            python_callable= python_callable.json_formatter
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
            task_id= 'empty_task'
        )



        can_run_batch_task >> rail.Label('Yes') >> batch_task >> empty_task

        can_run_batch_task >> rail.Label('No') >> get_last_sync_time >> get_clients >> has_list_data >>\
        rail.Label("Yes") >> formatted_client_names >> search_customer >> parse_qbo_customer >> is_customer_not_present >> rail.Label("Yes") >>\
        json_formatter >> process_child_dag >> wait_for_child >> update_lastsync_time

        is_customer_not_present >> rail.Label("No") >> empty_task

        has_list_data >> rail.Label('No') >> empty_task



rail.for_each_instance(create_main_airflow_dag)