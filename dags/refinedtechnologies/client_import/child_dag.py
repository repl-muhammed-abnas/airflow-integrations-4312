from datetime import timedelta
import rail
from airflow.models import Variable
from refinedtechnologies.client_import.utils import custom_function, request_payload, request_query

def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_client_child_dag_id,
        description=f'Refined Technologies Inc Client Import - Child DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,

    ) as dag:
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        # Batch the whole flow into one task when the toggle Variable is enabled.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_user_in_salesforce'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_user_in_salesforce',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        search_user_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_user_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda dag_run: request_query.search_user_in_salesforce_query(dag_run.conf.get('salesforce_record', {})),
        )

        search_contact_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_contact_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda dag_run: request_query.search_contact_in_salesforce_query(dag_run.conf.get('salesforce_record', {})),
        )

        search_client_by_code = rail.RepliconServiceOperator(
            task_id='search_client_by_code',
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda dag_run: request_payload.search_client_by_code_payload(dag_run.conf.get('salesforce_record', {})),
            data_handler=custom_function.get_clients_list
        )

        get_matching_client_uris = rail.PythonOperator(
            task_id='get_matching_client_uris',
            python_callable=lambda dag_run: custom_function.check_uri_presence_result(
                rail.result("search_client_by_code"),
                dag_run.conf.get('salesforce_record', {})['Legacy_Id__c']
            )
        )

        # Client code exists -> check active status; not found -> create a new client.
        check_client_exists = rail.IfOperator(
            task_id='check_client_exists',
            test=lambda: len(rail.result('get_matching_client_uris')) > 0,
            yes_task='check_client_active',
            no_task='search_replicon_user'
        )

        # Existing client active -> UPDATE; inactive -> create a new client + set manager.
        check_client_active = rail.IfOperator(
            task_id='check_client_active',
            test=lambda dag_run: custom_function.is_matching_client_active(
                rail.result("search_client_by_code"),
                dag_run.conf.get('salesforce_record', {})['Legacy_Id__c']
            ),
            yes_task='update_existing_client',
            no_task='search_replicon_user'
        )

        update_existing_client = rail.RepliconServiceOperator(
            task_id="update_existing_client",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda dag_run: request_payload.update_client_payload(
                dag_run.conf.get('salesforce_record', {}),
                rail.result("get_matching_client_uris")[0],
                dag_run.conf.get('countries', {}),
                rail.result('search_contact_in_salesforce')
            )
        )

        search_replicon_user = rail.RepliconServiceOperator(
            task_id='search_replicon_user',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda: request_payload.search_user_replicon_payload(
                rail.result("search_user_in_salesforce")['records'][0].get('Username')
                if rail.result("search_user_in_salesforce").get('records') else None
            )
        )

        create_new_client = rail.RepliconServiceOperator(
            task_id="create_new_client",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda dag_run: request_payload.create_new_client_payload(
                dag_run.conf.get('salesforce_record', {}),
                dag_run.conf.get('countries', {}),
                rail.result('search_contact_in_salesforce')
            )
        )

        check_username_and_uri_exists = rail.IfOperator(
            task_id='check_username_and_uri_exists',
            test=lambda: len(custom_function.get_uri_if_present(
                rail.result("search_replicon_user"),
                rail.result("search_user_in_salesforce")['records'][0]['Username']
                if rail.result("search_user_in_salesforce").get('records') else None
            )) > 0,
            yes_task='update_client_manager',
            no_task='log_to_sumo'
        )

        update_client_manager = rail.RepliconServiceOperator(
            task_id="update_client_manager",
            endpoint="/services/ClientService1.svc/UpdateClientManager",
            data=lambda: request_payload.update_manager_payload(
                rail.result("create_new_client")["uri"],
                custom_function.get_uri_if_present(
                    rail.result("search_replicon_user"),
                    rail.result("search_user_in_salesforce")['records'][0]['Username']
                    if rail.result("search_user_in_salesforce").get('records') else None
                )[0]
            )
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> search_user_in_salesforce

        search_user_in_salesforce >> search_contact_in_salesforce >> search_client_by_code
        search_client_by_code >> get_matching_client_uris >> check_client_exists

        check_client_exists >> rail.Label("Yes") >> check_client_active
        check_client_exists >> rail.Label("No") >> search_replicon_user

        check_client_active >> rail.Label("Yes") >> update_existing_client >> log_to_sumo
        check_client_active >> rail.Label("No") >> search_replicon_user

        search_replicon_user >> create_new_client >> check_username_and_uri_exists
        check_username_and_uri_exists >> rail.Label("Yes") >> update_client_manager >> log_to_sumo
        check_username_and_uri_exists >> rail.Label("No") >> log_to_sumo


    return dag


rail.for_each_instance(create_child_dag)
