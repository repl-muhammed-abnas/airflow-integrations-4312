from datetime import timedelta
import rail
from airflow.models import Variable
from refinedtechnologies.project_sync.utils import custom_function, request_payload, request_query


def create_child_dag(config):
    """Sub-child DAG that searches for, or creates, a Replicon client and returns its URI."""
    with rail.create_airflow_dag(
        dag_id=config.search_client_replicon_child_dag_id,
        description=f'Refined Technologies Project Sync - Child DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        view_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        # Batch the whole flow into one task when the toggle Variable is enabled.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='extract_salesforce_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='extract_salesforce_data',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        extract_salesforce_data = rail.PythonOperator(
            task_id='extract_salesforce_data',
            python_callable=lambda dag_run: custom_function.safe_get_salesforce_record(
                dag_run.conf.get('salesforce_data', {})
            ) or {}
        )

        get_clients_by_code = rail.RepliconServiceOperator(
            task_id='get_clients_by_code',
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda: request_payload.search_client_by_code_payload(rail.result("extract_salesforce_data")),
            data_handler=custom_function.convert_ruby_data_to_list
        )

        check_client_matches = rail.IfOperator(
            task_id='check_client_matches',
            test=lambda: custom_function.has_matching_client(rail.result("get_clients_by_code"), rail.result("extract_salesforce_data")),
            no_task='query_account_details',
            yes_task='log_existing_client_found'
        )

        log_existing_client_found = rail.WriteLogOperator(
            task_id='log_existing_client_found',
            message="successfully processed",
            severity='Success',
            properties={
                'status': 'Success',
            }
        )

        query_account_details = rail.SalesforceQueryOperator2(
            task_id='query_account_details',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.specific_account_query(rail.result("extract_salesforce_data"))
        )

        query_account_owner = rail.SalesforceQueryOperator2(
            task_id='query_account_owner',
            salesforce_conn_id=config.salesforce_conn_id,
            query = lambda: request_query.search_user_in_salesforce(
                custom_function.safe_get_salesforce_record(rail.result("query_account_details")) or {}
            ),
        )

        search_contact_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_contact_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_contact_in_salesforce_query(rail.result('query_account_details')),
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id='get_all_countries',
            endpoint="/services/InternationalizationService1.svc/GetAllCountries"
        )
        
        search_replicon_user = rail.RepliconServiceOperator(
            task_id='search_replicon_user',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: request_payload.search_user_payload(rail.result("query_account_owner")),
        )

        create_new_client = rail.RepliconServiceOperator(
            task_id="create_new_client",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda: request_payload.search_user_create_client_payload(rail.result('query_account_details'), rail.result('search_contact_in_salesforce'), rail.result("get_all_countries"))
        )

        get_client_manager_uri = rail.PythonOperator(
            task_id='get_client_manager_uri',
            python_callable=lambda: custom_function.extract_uri_from_rows(rail.result("search_replicon_user"), rail.result("query_account_owner"))
        )

        check_client_manager_exists = rail.IfOperator(
            task_id='check_client_manager_exists',
            test=lambda: len(rail.result("get_client_manager_uri")) >0,
            no_task='skip_client_manager',
            yes_task='update_client_manager'
        )

        update_client_manager = rail.RepliconServiceOperator(
            task_id='update_client_manager',
            endpoint="/services/ClientService1.svc/UpdateClientManager",
            data=lambda: request_payload.update_client_manager_payload(rail.result("create_new_client"), rail.result("get_client_manager_uri")),
        )

        skip_client_manager = rail.WriteLogOperator(
            task_id='skip_client_manager',
            message="successfully processed",
            severity='Success',
            properties={
                'status': 'Success',
            }
        )

        # Return clienturi / clientstatus / clientname to the parent DAG.
        return_result = rail.PythonOperator(
            task_id='return_result',
            python_callable=lambda: custom_function.build_search_client_reply(
                rail.result("get_clients_by_code"),
                rail.result("extract_salesforce_data"),
                rail.result("create_new_client") if rail.result("create_new_client") else None
            )
        )

        # Terminal task / batch end boundary.
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> extract_salesforce_data

        extract_salesforce_data  >> get_clients_by_code >> check_client_matches
        check_client_matches >> rail.Label("Yes") >> log_existing_client_found >> return_result
        check_client_matches >> rail.Label("No") >> query_account_details >> query_account_owner >> search_contact_in_salesforce >> get_all_countries >> search_replicon_user >> create_new_client >> get_client_manager_uri >> check_client_manager_exists
        check_client_manager_exists >> rail.Label("Yes") >> update_client_manager >> return_result
        check_client_manager_exists >> rail.Label("No") >> skip_client_manager >> return_result
        return_result >> log_to_sumo
    

    return dag


rail.for_each_instance(create_child_dag)
