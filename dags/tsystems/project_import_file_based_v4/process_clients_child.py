from datetime import timedelta
import rail
from tsystems.project_import_file_based_v4.utils import request_payload, response_filter
from airflow.models import Variable

def create_process_clients_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_clients_dag_id,
        description='T-Systems Process Clients Child DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:
        
        # View DAG run configuration
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Batch task wrapper for error handling
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='extract_client_info'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='extract_client_info',
            end_task='catch_and_log_errors',
        )

        # Version 1.7: Extract client code and name, build formatted name
        extract_client_info = rail.PythonOperator(
            task_id='extract_client_info',
            python_callable=lambda dag_run: {
                'clientcode': dag_run.conf['client_code'],
                'client_name': f"{dag_run.conf['client_code']}_{dag_run.conf['client_name']}"
                    if dag_run.conf.get('client_name', '') else f"Unknown({dag_run.conf['client_code']})"
            }
        )

        # Check if client already exists in Replicon
        get_existing_clients = rail.RepliconServiceOperator(
            task_id = 'get_existing_clients',
            endpoint="/services/ClientListService1.svc/GetData",
            data = request_payload.get_client_data,
            data_handler=response_filter.get_client_data_from_list_service
        )

        # Decision: Create or update client
        client_exists = rail.IfOperator(
            task_id='client_exists',
            test='{{ result("get_existing_clients") | is_truthy }}',
            yes_task='should_update_client_name',
            no_task='create_client'
        )

        # Version 1.7: Check if client name needs to be updated
        should_update_client_name = rail.IfOperator(
            task_id='should_update_client_name',
            test=lambda: rail.result("get_existing_clients")['name'] != rail.result('extract_client_info')['client_name'],
            yes_task='update_client_name',
            no_task='log_client_exists'
        )

        # Version 1.7: Update existing client name
        update_client_name = rail.RepliconServiceOperator(
            task_id='update_client_name',
            endpoint='/services/ClientService1.svc/UpdateName',
            data=lambda: {
                "clientUri": rail.result("get_existing_clients")['uri'],
                "name": rail.result('extract_client_info')['client_name']
            }
        )

        # Log successful client name update
        log_client_name_updated = rail.WriteLogOperator(
            task_id="log_client_name_updated",
            severity="Success",
            log='{{ dag_run.conf.main_log }}',
            message="Client name updated successfully",
            properties=lambda dag_run: {
                "projectid": '',
                "projectname": '',
                'clientcode': dag_run.conf['client_code'],
                'details': f"Client name updated to: {rail.result('extract_client_info')['client_name']}",
                'action': 'Update',
                "status": 'Success'
            }
        )

        # Log that client already exists (no update needed)
        log_client_exists = rail.WriteLogOperator(
            task_id="log_client_exists",
            severity="Info",
            message="Client already exists, no action needed",
            properties=lambda dag_run: {
                'clientcode': dag_run.conf['client_code'],
                'details': 'Client already exists in Replicon'
            }
        )

        # Create new client with "Unknown" prefix
        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.get_create_client_payload
        )

        # Log successful client creation
        log_client_created = rail.WriteLogOperator(
            task_id="log_client_created",
            severity="Success",
            log='{{ dag_run.conf.main_log }}',
            message="Client created successfully",
            properties=lambda dag_run: {
                "projectid": '',
                "projectname": '',
                'clientcode': dag_run.conf['client_code'],
                'details': 'Client created successfully',
                'action': 'Add',
                "status": 'Success'
            }
        )

        # Error handling and logging
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.main_log }}',
            message='{{ get_error_message() }}',
            severity='Error',
            properties=lambda dag_run: {
                "projectid": '',
                "projectname": '',
                'clientcode': dag_run.conf['client_code'],
                'details': rail.render_template('{{ get_error_message() }}'),
                'action': 'Add',
                'status': 'Error'
            }
        )

        # Define task dependencies
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> extract_client_info

        extract_client_info >> get_existing_clients >> client_exists

        client_exists >> rail.Label("Yes") >> should_update_client_name
        should_update_client_name >> rail.Label("Yes") >> update_client_name >> log_client_name_updated >> catch_and_log_errors
        should_update_client_name >> rail.Label("No") >> log_client_exists >> catch_and_log_errors
        client_exists >> rail.Label("No") >> create_client >> log_client_created >> catch_and_log_errors

    return dag

rail.for_each_instance(create_process_clients_dag)