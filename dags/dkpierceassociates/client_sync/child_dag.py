from datetime import timedelta
import rail
from dkpierceassociates.client_sync.utils import custom_function, request_payload, request_query


def create_child_dag(config):
    """
    Child DAG for processing each Salesforce account record.
    Triggered from master DAG via TriggerDagRunForEachItemOperator.
    """
    with rail.create_airflow_dag(
        dag_id=config.process_account_dag_id,
        description='Sync accounts from Salesforce to Replicon - Child DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Extract account record from dag_run.conf
        extract_account_data = rail.PythonOperator(
            task_id='extract_account_data',
            python_callable=lambda dag_run: dag_run.conf.get('account_record', {})
        )

        # Create a wrapper to return full structure like original
        prepare_salesforce_data = rail.PythonOperator(
            task_id='prepare_salesforce_data',
            python_callable=lambda: {
                'records': [rail.result('extract_account_data')],
                'totalSize': 1
            }
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id='get_all_countries',
            endpoint="/services/InternationalizationService1.svc/GetAllCountries"
        )

        check_client_manager_presence = rail.IfOperator(
            task_id='check_client_manager_presence',
            test=lambda: len(custom_function.check_client_manager_hidden_is_not_present(rail.result("prepare_salesforce_data"))) > 0,
            no_task='check_client_id_hiden_presence',
            yes_task='search_replicon_client_manager'
        )

        check_client_id_hiden_presence = rail.IfOperator(
            task_id='check_client_id_hiden_presence',
            test=lambda: len(custom_function.check_client_id_hiden_is_not_present(rail.result("prepare_salesforce_data"))) > 0,
            no_task='create_client',
            yes_task='get_client_details_from_replicon'
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda: request_payload.create_client_payload(
                rail.result("prepare_salesforce_data")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_client = rail.RepliconServiceOperator(
            task_id='update_client',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda: request_payload.update_client_payload(
                rail.result("prepare_salesforce_data")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_client_details_from_replicon = rail.RepliconServiceOperator(
            task_id='get_client_details_from_replicon',
            endpoint="/services/ClientService1.svc/GetClientDetails",
            data=lambda: request_payload.get_client_details_from_replicon_payload(
                rail.result("prepare_salesforce_data")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_account_in_salesforce = rail.SalesforceUpdateObjectOperator2(
            task_id='update_account_in_salesforce',
            operation= 'update',
            object_name= 'Account',
            payload= request_payload.update_account_salesforce_payload,
            salesforce_conn_id=config.salesforce_conn_id,
        )

        search_replicon_client_manager = rail.SalesforceQueryOperator2(
            task_id='search_replicon_client_manager',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_replicon_client_managers_query(
                rail.result("prepare_salesforce_data")
            ),
        )
        
        extract_account_data >> prepare_salesforce_data >> get_all_countries >> check_client_manager_presence
        check_client_manager_presence >> rail.Label("client manager is present") >> search_replicon_client_manager >> check_client_id_hiden_presence
        check_client_manager_presence >> rail.Label("client manager is not present") >> check_client_id_hiden_presence
        check_client_id_hiden_presence >> rail.Label("client id hiden is present") >> get_client_details_from_replicon >> update_client
        check_client_id_hiden_presence >> rail.Label("client id hiden is not present") >> create_client >> update_account_in_salesforce

    return dag


# Create child DAG for each instance
rail.for_each_instance(create_child_dag)