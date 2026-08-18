from datetime import timedelta
import rail
from tsystems.project_import_file_based.utils import request_payload, response_filter
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

        # Get client code from configuration
        extract_client_info = rail.PythonOperator(
            task_id='extract_client_info',
            python_callable=lambda dag_run: {
                'clientcode': dag_run.conf['client_code'],
                'client_name': f"Unknown({dag_run.conf['client_code']})"
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
            yes_task='log_client_exists',
            no_task='create_client'
        )

        # Log that client already exists
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
            endpoint='/services/ClientService1.svc/PutClient',
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

        client_exists >> rail.Label("Yes") >> log_client_exists >> catch_and_log_errors
        client_exists >> rail.Label("No") >> create_client >> log_client_created >> catch_and_log_errors

    return dag

rail.for_each_instance(create_process_clients_dag)