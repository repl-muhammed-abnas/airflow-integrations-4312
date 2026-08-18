from datetime import datetime, timedelta
import rail
from tsystems.project_billing_rate_import_v1.utils import custom_methods

open_bracket = '{{'
close_bracket = '}}'


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.api_master_dag_id,
        description='Tsystems Project Billing Rate Assignment API Master Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 1, 1),
        max_active_runs=config.max_active_runs_api_master,
        schedule_interval=timedelta(minutes=config.master_dag_interval),
    ) as dag:

        # Fetch project billing rate event data from external API with comprehensive error handling
        fetch_billing_event = rail.SimpleHttpOperator(
            task_id='fetch_billing_event',
            method='GET',
            http_conn_id=config.project_billing_rate_import_http_conn_id,
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {open_bracket}var.value.{config.access_token}{close_bracket}'
            }
        )

        # Handle API response status for create projects endpoint
        # Treats 503/504 errors as success (no data available), genuine errors as failures
        get_project_billing_rate_import_api_status = rail.PythonOperator(
            task_id='get_project_billing_rate_import_api_status',
            trigger_rule='all_done',
            python_callable=custom_methods.handle_api_error_504
        )

        # Determine if API returned processable data
        # Routes to transformation if data available, skips to validate run if no data
        if_project_billing_rate_import_api_has_data = rail.IfOperator(
            task_id='if_project_billing_rate_import_api_has_data',
            test=lambda: bool(rail.result(
                "get_project_billing_rate_import_api_status")['process']),
            yes_task='transform_billing_rate_records_list',
            no_task='validate_final_status'
        )

        # Parse concatenated JSON response and transform to standardized list
        # Handles event wrapper structure and extracts billing rate data for processing
        transform_billing_rate_records_list = rail.PythonOperator(
            task_id='transform_billing_rate_records_list',
            python_callable=lambda: custom_methods.parse_and_transform_api_response_to_billing_rate_records_list(
                rail.result('fetch_billing_event'), config)
        )

        # Determine if parsed billing rate records list contains any records
        # Routes to bulk processing if projects found, continues to update API if empty
        has_billing_rate_records = rail.IfOperator(
            task_id='has_billing_rate_records',
            test=lambda: len(rail.result(
                'transform_billing_rate_records_list')) > 0,
            yes_task='trigger_project_billing_rate_assignment_master_dag',
            no_task='validate_final_status'
        )

        trigger_project_billing_rate_assignment_master_dag = rail.TriggerDagRunOperator(
            task_id='trigger_project_billing_rate_assignment_master_dag',
            trigger_dag_id=config.master_dag_id,
            conf=lambda: {
                "billing_event_records_list": rail.result('transform_billing_rate_records_list')
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_trigger_project_billing_rate_assignment_master_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_project_billing_rate_assignment_master_dag',
            dag_runs="{{ result('trigger_project_billing_rate_assignment_master_dag') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Final validation of integration success status
        # Fails DAG if either API had genuine errors (excludes 503/504 as successful)
        validate_final_status = rail.PythonOperator(
            task_id='validate_final_status',
            python_callable=custom_methods.validate_integration_success
        )

        fetch_billing_event >> get_project_billing_rate_import_api_status >> if_project_billing_rate_import_api_has_data

        if_project_billing_rate_import_api_has_data >> rail.Label(
            "No") >> validate_final_status
        if_project_billing_rate_import_api_has_data >> rail.Label(
            "Yes") >> transform_billing_rate_records_list >> has_billing_rate_records

        has_billing_rate_records >> rail.Label(
            "No") >> validate_final_status
        has_billing_rate_records >> rail.Label(
            "No") >> trigger_project_billing_rate_assignment_master_dag >> wait_for_trigger_project_billing_rate_assignment_master_dag >> validate_final_status

    return dag


rail.for_each_instance(create_main_dag)
