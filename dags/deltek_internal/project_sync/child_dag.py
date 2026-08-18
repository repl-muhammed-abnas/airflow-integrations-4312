
from datetime import timedelta
import rail
from airflow.models import Variable
from deltek_internal.project_sync import config
from deltek_internal.project_sync.utils import request_payload

from deltek_internal.project_sync.utils import custom_functions

def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"{config.company_key}_salesforce_to_polaris_project_sync_child_{config.instance}",
        description=f'Salesforce to Polaris Project Sync Child - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,  # Triggered by master DAG
        max_active_runs=config.max_active_run_child,
    ) as dag:

        # View incoming configuration from master DAG
        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config'
        )

        extract_record = rail.PythonOperator(
            task_id='extract_record',
            python_callable=lambda dag_run: dag_run.conf.get('salesforce_record', {})
        )
        
        get_account_name_from_salesforce = rail.InternalSalesforceQueryOperator(
            task_id='get_account_name_from_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda : request_payload.get_account_name_from_salesforce_query(rail.result('extract_record')),
        )

        get_caseNumber_from_salesforce = rail.InternalSalesforceQueryOperator(
            task_id='get_caseNumber_from_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda : request_payload.get_case_number_from_salesforce_query(rail.result('extract_record')),
        )

        search_client_in_polaris = rail.RepliconServiceOperator(
            task_id='search_client_in_polaris',
            endpoint='/services/ClientListService1.svc/GetData',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_search_client_payload(
                rail.result('get_account_name_from_salesforce')),
        )

        # Check if client exists
        client_exists = rail.IfOperator(
            task_id='client_exists',
            test=lambda: len(rail.result("search_client_in_polaris")['rows']) > 0,
            yes_task='collect_client_uri',
            no_task='create_client_in_polaris'
        )

        # Create client if it doesn't exist
        create_client_in_polaris = rail.RepliconServiceOperator(
            task_id='create_client_in_polaris',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_create_client_payload(rail.result('get_account_name_from_salesforce')),
            # data_handler=lambda response: response.get('client', {}).get('uri')
        )

        def get_client_uri():
            """
            Get client URI from either search result or create result
            """
            # Check if client was created (this task will have run if client didn't exist)
            try:
                created_uri = rail.result('create_client_in_polaris')
                if created_uri:
                    return created_uri["uri"]
            except:
                pass

            # Otherwise, get from search results (client existed)
            search_result = rail.result('search_client_in_polaris')

            if search_result and 'rows' in search_result and len(search_result['rows']) > 0:
                # The client URI should be in the first row
                # First cell (index 0) corresponds to 'Client' column
                row = search_result['rows'][0]
                if 'cells' in row and len(row['cells']) > 0:
                    client_cell = row['cells'][1]  # First column is 'Client'
                    # URI is directly in the cell, not nested under 'value'
                    if 'uri' in client_cell:
                        if client_cell["textValue"] == rail.result('get_account_name_from_salesforce').get('records')[0].get("Name"):
                            return client_cell['uri']

            raise ValueError("Could not find client URI in either create or search results")

        collect_client_uri = rail.PythonOperator(
            task_id='collect_client_uri',
            python_callable=get_client_uri
        )

        # Use BulkGetProjectDetails2 to search for existing project by name
        search_existing_project = rail.RepliconServiceOperator(
            task_id='search_existing_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails2',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda : request_payload.build_search_existing_project_payload(rail.result('extract_record')),
            data_handler=lambda response: custom_functions.get_projects_list(response)
        )

        project_exists = rail.IfOperator(
            task_id='project_exists',
            test=lambda: len(rail.result('search_existing_project')) > 0,
            yes_task='create_project_exist_log',
            no_task='get_project_template'
        )

        create_project_exist_log = rail.EmptyOperator(
            task_id="create_project_exist_log"
        )

        get_project_template = rail.RepliconServiceOperator(
            task_id='get_project_template',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails2',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda : request_payload.build_get_project_template_payload(),
            data_handler=lambda response: custom_functions.get_projects_list(response)
        )

        create_duplicate_project = rail.RepliconServiceOperator(
            task_id='create_duplicate_project',
            endpoint='/services/ProjectService1.svc/CreateProjectCopyBatch2',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda : request_payload.build_create_duplicate_project_payload(rail.result('extract_record')),
        )

        processing_batch_in_background = rail.RepliconServiceOperator(
            task_id='processing_batch_in_background',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda : request_payload.build_processing_batch_in_background_payload(rail.result("create_duplicate_project")),
        )

        batch_get_status = rail.RepliconServiceOperator(
            task_id='batch_get_status',
            endpoint='/services/BatchManagementService1.svc/GetStatus',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda : request_payload.build_processing_batch_in_background_payload(rail.result("create_duplicate_project")),
        )

        # Modify the duplicated project with opportunity data
        modify_duplicate_project = rail.RepliconServiceOperator(
            task_id='modify_duplicate_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda : request_payload.build_duplicate_project_payload(
                rail.result("extract_record"), 
                rail.result("get_project_template"),
                rail.result("get_caseNumber_from_salesforce")
            ),
        )

        update_client = rail.RepliconServiceOperator(
            task_id='update_client',
            endpoint='/services/ProjectService1.svc/UpdateClients',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda : request_payload.build_update_client_payload(
            ),
        )
        
        getProjectWorkflowStateActions = rail.RepliconServiceOperator(
            task_id='getProjectWorkflowStateActions',
            endpoint='/services/ProjectService1.svc/GetProjectWorkflowStateActions',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda : request_payload.build_getProjectWorkflowStateActions(),
        )

        performProjectWorkflowAction = rail.RepliconServiceOperator(
            task_id='performProjectWorkflowAction',
            endpoint='/services/ProjectService1.svc/PerformProjectWorkflowAction',
            replicon_conn_id=config.replicon_conn_id,
            data=lambda : request_payload.build_performProjectWorkflowAction(),
        )

        # Return processing result for master DAG to gather
        def build_processing_result():
            """
            Build a structured result for the master DAG to collect
            Returns information about what was processed
            """
            record = rail.result('extract_record')
            opportunity_name = record[0].get('Name', 'Unknown') if record else 'Unknown'

            # Check if project already existed or was created
            existing_projects = rail.result('search_existing_project')
            project_already_existed = len(existing_projects) > 0 if existing_projects else False

            return {
                'opportunity_name': opportunity_name,
                'status': 'success',
                'action': 'already_exists' if project_already_existed else 'created',
                'client_name': record[0]["Account"]["Name"]
            }

        processing_result = rail.PythonOperator(
            task_id='processing_result',
            python_callable=build_processing_result
        )

        view_dagrun_config

        # Main processing flow
        extract_record >> get_account_name_from_salesforce >> get_caseNumber_from_salesforce >> search_client_in_polaris
        search_client_in_polaris >> client_exists

        # Client creation path
        client_exists >> rail.Label("No") >> create_client_in_polaris >> collect_client_uri
        client_exists >> rail.Label("Yes") >> collect_client_uri

        # Continue from collect_client_uri
        collect_client_uri >> search_existing_project >> project_exists

        # New project creation path
        project_exists >> rail.Label("No") >> get_project_template >> create_duplicate_project >> processing_batch_in_background >> batch_get_status >>modify_duplicate_project >> update_client >> getProjectWorkflowStateActions >> performProjectWorkflowAction >> processing_result

        # Update existing project path
        project_exists >> rail.Label("Yes") >> create_project_exist_log >> processing_result

    return dag


# Create child DAG for each instance
rail.for_each_instance(create_child_dag)