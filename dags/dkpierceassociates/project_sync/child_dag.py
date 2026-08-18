from datetime import timedelta
import rail
from dkpierceassociates.project_sync.utils import custom_function, request_payload, request_query


def create_child_dag(config):
    """
    Child DAG for processing each Salesforce opportunity record.
    Triggered from master DAG via TriggerDagRunForEachItemOperator.
    """
    with rail.create_airflow_dag(
        dag_id=config.process_opportunity_dag_id,
        description='Sync opportunities from Salesforce as projects to Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Extract opportunity record from dag_run.conf
        extract_opportunity_data = rail.PythonOperator(
            task_id='extract_opportunity_data',
            python_callable=lambda dag_run: dag_run.conf.get('opportunity_record', {})
        )

        # Create a wrapper to return full structure like original
        prepare_salesforce_data = rail.PythonOperator(
            task_id='prepare_salesforce_data',
            python_callable=lambda: {
                'records': [rail.result('extract_opportunity_data')],
                'totalSize': 1
            }
        )

        search_account_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_account_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_account_in_salesforce_query(
                rail.result("prepare_salesforce_data")
            ),
        )

        get_all_project_status_labels = rail.RepliconServiceOperator(
            task_id='get_all_project_status_labels',
            endpoint="/services/ProjectStatusService1.svc/GetAllProjectStatusLabels",
            replicon_conn_id=config.replicon_conn_id,
        )

        project_id_hidden_is_not_present = rail.PythonOperator(
            task_id='project_id_hidden_is_not_present',
            python_callable=lambda: custom_function.check_project_id_hidden_is_not_present(
                rail.result("prepare_salesforce_data")
            )
        )

        check_project_id_hidden_is_not_present = rail.IfOperator(
            task_id='check_project_id_hidden_is_not_present',
            test=lambda: len(rail.result('project_id_hidden_is_not_present')) > 0,
            no_task='check_project_manager_is_not_present',
            yes_task='project_id_hidden_is_present'
        )
        
        check_project_manager_is_not_present = rail.IfOperator(
            task_id='check_project_manager_is_not_present',
            test=lambda: len(custom_function.check_project_manager_is_not_present(rail.result("prepare_salesforce_data"))) < 1,
            no_task='searchRepliconProjectManagers',
            yes_task='create_project'
        )

        project_id_hidden_is_present = rail.EmptyOperator(
            task_id='project_id_hidden_is_present'
        )

        searchRepliconProjectManagers = rail.SalesforceQueryOperator2(
            task_id='searchRepliconProjectManagers',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.searchRepliconProjectManagers_query(
                rail.result("prepare_salesforce_data")
            ),
        )

        validate_project_manager_found = rail.PythonOperator(
            task_id='validate_project_manager_found',
            python_callable=lambda: custom_function.validate_project_manager_exists(
                rail.result("searchRepliconProjectManagers")
            )
        )

        searchRepliconProjectManager = rail.SalesforceQueryOperator2(
            task_id='searchRepliconProjectManager',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.searchRepliconProjectManagers_query(
                rail.result("prepare_salesforce_data")
            ),
        )

        create_project = rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda: request_payload.create_project_payload(
                rail.result("prepare_salesforce_data")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_client_details = rail.RepliconServiceOperator(
            task_id='get_client_details',
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda: request_payload.search_client_by_name_payload(rail.result("search_account_in_salesforce")),
            replicon_conn_id=config.replicon_conn_id,
        )
        getAllUserTeamMemberUri = rail.RepliconServiceOperator(
            task_id='getAllUserTeamMemberUri',
            endpoint="/services/ProjectService1.svc/GetAllUserTeamMemberUri",
            data={},
            replicon_conn_id=config.replicon_conn_id,
        )

        assign_resource_to_project = rail.RepliconServiceOperator(
            task_id='assign_resource_to_project',
            endpoint="/services/ProjectService1.svc/AssignResourceToProject",
            data=lambda: request_payload.assign_resource_to_project_payload(rail.result("create_project")),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_opportunity_in_salesforce = rail.SalesforceUpdateObjectOperator2(
            task_id='update_opportunity_in_salesforce',
            operation= 'update',
            object_name= 'Opportunity',
            payload= request_payload.update_opportunity_salesforce_payload,
            salesforce_conn_id=config.salesforce_conn_id,
        )
 
        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjects",
            data=lambda: request_payload.get_project_details_payload(rail.result("extract_opportunity_data")),
            replicon_conn_id=config.replicon_conn_id,
        )

        if_project_manager_is_not_present = rail.IfOperator(
            task_id='if_project_manager_is_not_present',
            test=lambda: len(custom_function.check_project_manager_is_not_present(rail.result("prepare_salesforce_data"))) < 1,
            no_task='searchRepliconProjectManager',
            yes_task='update_project'
        )

        update_project = rail.RepliconServiceOperator(
            task_id='update_project',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda: request_payload.update_project_payload(
                rail.result("prepare_salesforce_data")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )
        extract_opportunity_data >> prepare_salesforce_data >> get_all_project_status_labels >> search_account_in_salesforce >> project_id_hidden_is_not_present >> check_project_id_hidden_is_not_present
        
        check_project_id_hidden_is_not_present >> rail.Label("No") >> check_project_manager_is_not_present
        check_project_id_hidden_is_not_present >> rail.Label("Yes") >> project_id_hidden_is_present 
        
        check_project_manager_is_not_present >> rail.Label("Yes") >> create_project >> get_client_details >> getAllUserTeamMemberUri >> assign_resource_to_project >> update_opportunity_in_salesforce
        check_project_manager_is_not_present >> rail.Label("No") >> searchRepliconProjectManagers >> validate_project_manager_found >> create_project
    
        project_id_hidden_is_present >> get_project_details >> if_project_manager_is_not_present 
        
        if_project_manager_is_not_present >> rail.Label("Yes") >> update_project
        if_project_manager_is_not_present >> rail.Label("No") >> searchRepliconProjectManager >> update_project
    return dag


# Create child DAG for each instance
rail.for_each_instance(create_child_dag)