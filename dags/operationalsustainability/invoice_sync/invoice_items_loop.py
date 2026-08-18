import rail
import re
from operationalsustainability.invoice_sync.utils import python_callable
from operationalsustainability.invoice_sync import config


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.invoice_items_loop_dag_id,
        description= 'Sync new invoice in Replicon to QuickBooks_Invoice Items Loop',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:
    
        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')
        
        is_billing_type_notequal_expense = rail.IfOperator(
            task_id='is_billing_type_notequal_expense',
            test="{{ dag_run.conf.billingType.split(':')[-1] != 'expense' }}",
            yes_task='is_uri_present',
            no_task='is_billing_type_expense'
        )

        is_uri_present = rail.IfOperator(
            task_id='is_uri_present',
            test=lambda dag_run: bool(dag_run.conf.get('project', {}).get('uri')),
            yes_task='get_project_details',
            no_task='search_items_qbo_2'
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: {
              "projects": [
                {
                  "uri": dag_run.conf['project']['uri']
                }
              ]
          }
        )

        search_items_qbo_1 = rail.InternalQuickbooksAPIOperator(
            task_id='search_items_qbo_1',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id= config.qbo_conn_id,
            query_params=lambda dag_run: {
                'query': f"SELECT * FROM Item WHERE Active = true and Name= '{python_callable.escape_sql_string(dag_run.conf.get('custom_products_standard_price_book', ''))}'"
            }
        )

        invoice_data = []

        append_to_invoice_data_uri_present = rail.PythonOperator(
            task_id='append_to_invoice_data_uri_present',
            python_callable= lambda dag_run: python_callable.add_to_invoice_data(dag_run, invoice_data, rail.result('search_items_qbo_1'))
        )

        search_items_qbo_2 = rail.InternalQuickbooksAPIOperator(
            task_id='search_items_qbo_2',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id= config.qbo_conn_id,
            query_params=lambda dag_run: {
                'query': "SELECT * FROM Item WHERE Active = true and Name= 'Expense Processing Fee'"
            }
        )

        append_to_invoice_data_uri_not_present = rail.PythonOperator(
            task_id='append_to_invoice_data_uri_not_present',
            python_callable= lambda dag_run: python_callable.add_to_invoice_data(dag_run, invoice_data, rail.result('search_items_qbo_2'))
        )

        is_billing_type_expense = rail.IfOperator(
            task_id='is_billing_type_expense',
            test="{{ dag_run.conf.billingType.split(':')[-1] == 'expense' }}",
            yes_task='search_items_qbo_3',
            no_task='empty_task'
        )

        search_items_qbo_3 = rail.InternalQuickbooksAPIOperator(
            task_id='search_items_qbo_3',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id= config.qbo_conn_id,
            query_params=lambda dag_run: {
                'query': "SELECT * FROM Item WHERE Active = true and Name= 'Expense Processing Fee'"
            }
        )

        append_to_invoice_data_expense = rail.PythonOperator(
            task_id='append_to_invoice_data_expense',
            python_callable= lambda dag_run: python_callable.add_to_invoice_data(dag_run, invoice_data, rail.result('search_items_qbo_3'))
        )

        empty_task = rail.EmptyOperator(
            task_id= 'empty_task'
        )


        is_billing_type_notequal_expense >> rail.Label("Yes") >> is_uri_present >> rail.Label("Yes") >> get_project_details >>\
        search_items_qbo_1 >> append_to_invoice_data_uri_present

        is_uri_present >> rail.Label("No") >> search_items_qbo_2 >> append_to_invoice_data_uri_not_present

        is_billing_type_notequal_expense >> rail.Label("No") >> is_billing_type_expense >> rail.Label("Yes") >> search_items_qbo_3 >> append_to_invoice_data_expense

        is_billing_type_expense >> rail.Label("No") >> empty_task

        



rail.for_each_instance(create_child_dag)