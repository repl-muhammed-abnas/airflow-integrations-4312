from datetime import datetime, timedelta
import rail
from tsystems.project_import_v3.utils import custom_methods

open_bracket = '{{'
close_bracket = '}}'

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dag_id,
        description='Tsystems Project Import Webhook Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1),
        max_active_runs=config.master_max_active_run,
        schedule_interval=config.schedule_interval
    ) as dag:

        # Fetch project create data from external API with comprehensive error handling
        fetch_create_projects = rail.SimpleHttpOperator(
            task_id='fetch_create_projects',
            method='GET',
            endpoint= config.create_project_endpoint,
            http_conn_id = config.create_projects_http_conn_id,
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {open_bracket}var.value.{config.access_token}{close_bracket}'
            }
        )

        # Handle API response status for create projects endpoint
        # Treats 503/504 errors as success (no data available), genuine errors as failures
        get_create_projects_api_status = rail.PythonOperator(
            task_id = 'get_create_projects_api_status',
            trigger_rule = 'all_done',
            python_callable=lambda: custom_methods.handle_api_error_504('create')
        )

        # Determine if create API returned processable data
        # Routes to transformation if data available, skips to update API if no data
        if_create_projects_api_has_data = rail.IfOperator(
            task_id = 'if_create_projects_api_has_data',
            test = lambda: bool(rail.result("get_create_projects_api_status")['process']),
            yes_task = 'transform_create_to_project_list',
            no_task = 'fetch_update_projects'
        )

        # Parse concatenated JSON response and transform to standardized project list
        # Handles event wrapper structure and extracts costobject data for processing
        transform_create_to_project_list = rail.PythonOperator(
            task_id='transform_create_to_project_list',
            python_callable=lambda: custom_methods.parse_and_transform_api_response_to_project_list(
                rail.result('fetch_create_projects'), 'create'
            )
        )

        # Determine if parsed create projects list contains any records
        # Routes to bulk processing if projects found, continues to update API if empty
        has_create_projects = rail.IfOperator(
            task_id='has_create_projects',
            test=lambda: len(rail.result('transform_create_to_project_list')) > 0,
            yes_task='process_create_project_bulk',
            no_task='fetch_update_projects'
        )

        # Trigger master DAG to process create projects as a batch
        # Passes complete project list with operation type for bulk processing
        process_create_project_bulk = rail.TriggerDagRunOperator(
            task_id='process_create_project_bulk',
            trigger_dag_id=config.process_payload_dag_id,
            conf=lambda: {
                "project_list": rail.result('transform_create_to_project_list'),
                "operation_type": "create",
                "batch_size": len(rail.result('transform_create_to_project_list'))
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        # Wait for create project batch processing to complete before proceeding
        # Ensures all create operations finish before starting update operations
        wait_for_create_project_bulk = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_project_bulk',
            dag_runs="{{ result('process_create_project_bulk') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Fetch project update data from external API with comprehensive error handling
        fetch_update_projects = rail.SimpleHttpOperator(
            task_id='fetch_update_projects',
            method='GET',
            endpoint= config.update_project_endpoint,
            http_conn_id = config.update_projects_http_conn_id,
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {open_bracket}var.value.{config.access_token}{close_bracket}'
            },
        )

        # Handle API response status for update projects endpoint
        # Treats 503/504 errors as success (no data available), genuine errors as failures
        get_update_projects_api_status = rail.PythonOperator(
            task_id = 'get_update_projects_api_status',
            trigger_rule = 'all_done',
            python_callable=lambda: custom_methods.handle_api_error_504('update')
        )

        # Determine if update API returned processable data
        # Routes to transformation if data available, skips to final validation if no data
        if_update_projects_api_has_data = rail.IfOperator(
            task_id = 'if_update_projects_api_has_data',
            test = lambda: bool(rail.result("get_update_projects_api_status")['process']),
            yes_task = 'transform_update_to_project_list',
            no_task = 'validate_final_status'
        )

        # Parse concatenated JSON response and transform to standardized project list
        # Handles event wrapper structure and extracts costobject data for update processing
        transform_update_to_project_list = rail.PythonOperator(
            task_id='transform_update_to_project_list',
            python_callable=lambda: custom_methods.parse_and_transform_api_response_to_project_list(
                rail.result('fetch_update_projects'), 'update'
            )
        )

        # Determine if parsed update projects list contains any records
        # Routes to bulk processing if projects found, skips to validation if empty
        has_update_projects = rail.IfOperator(
            task_id='has_update_projects',
            test=lambda: len(rail.result('transform_update_to_project_list')) > 0,
            yes_task='process_update_project_bulk',
            no_task='validate_final_status'
        )

        # Trigger master DAG to process update projects as a batch
        # Passes complete project list with operation type for bulk processing
        process_update_project_bulk = rail.TriggerDagRunOperator(
            task_id='process_update_project_bulk',
            trigger_dag_id=config.process_payload_dag_id,
            conf=lambda: {
                "project_list": rail.result('transform_update_to_project_list'),
                "operation_type": "update",
                "batch_size": len(rail.result('transform_update_to_project_list'))
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        # Wait for update project batch processing to complete before final validation
        # Ensures all update operations finish before validating overall integration success
        wait_for_update_project_bulk = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_project_bulk',
            dag_runs="{{ result('process_update_project_bulk') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Final validation of integration success status
        # Fails DAG if either API had genuine errors (excludes 503/504 as successful)
        validate_final_status = rail.PythonOperator(
            task_id='validate_final_status',
            python_callable=lambda: custom_methods.validate_integration_success(),
            retries = 0
        )

        # Create API branches
        fetch_create_projects >> get_create_projects_api_status >> if_create_projects_api_has_data
        if_create_projects_api_has_data >> rail.Label('Yes') >> transform_create_to_project_list >> has_create_projects
        if_create_projects_api_has_data >> rail.Label('No') >> fetch_update_projects
        
        # Create processing branches
        has_create_projects >> rail.Label("Yes") >> process_create_project_bulk >> wait_for_create_project_bulk >> fetch_update_projects
        has_create_projects >> rail.Label("No") >> fetch_update_projects

        # Update API branches
        fetch_update_projects >> get_update_projects_api_status >> if_update_projects_api_has_data
        if_update_projects_api_has_data >> rail.Label("Yes") >> transform_update_to_project_list >> has_update_projects
        if_update_projects_api_has_data >> rail.Label("No") >> validate_final_status

        # Update processing branches
        has_update_projects >> rail.Label("Yes") >> process_update_project_bulk >> wait_for_update_project_bulk >> validate_final_status
        has_update_projects >> rail.Label("No") >> validate_final_status

    return dag


rail.for_each_instance(create_main_dag)
