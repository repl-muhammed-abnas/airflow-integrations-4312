"""
Guidehouse PeopleSoft Project Import - Client Processing Child DAG
"""
from datetime import timedelta
import pendulum
import rail
from airflow.models import Variable
from guidehouse.peoplesoft_project_import.utils import custom_method, response_filter, request_payload

def create_client_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_clients_dag_id,
        description='Guidehouse PeopleSoft Client Processing - Individual Client Handling',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name,
                default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_clients_in_replicon'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='get_clients_in_replicon',
            end_task='catch_and_log_errors',
        )

        get_clients_in_replicon = rail.RepliconServiceOperator(
            task_id='get_clients_in_replicon',
            endpoint="/services/ClientService1.svc/GetActiveClients",
            data_handler=lambda response, dag_run: response_filter.get_client_data(response, dag_run)
        )

        validate_input_data = rail.IfOperator(
            task_id='validate_input_data',
            test=lambda: rail.result("get_clients_in_replicon")["has_required_data"],
            yes_task='check_client_exists',
            no_task='log_validation_error'
        )

        log_validation_error = rail.WriteLogOperator(
            task_id='log_validation_error',
            log='{{ dag_run.conf.log }}',
            message='{{ result("get_clients_in_replicon")["validation_error"] }}',
            severity='Exception',
            properties=lambda dag_run: custom_method.get_client_log_properties(
                dag_run,
                'Validation (Client)',
                'Exception',
                rail.result("get_clients_in_replicon")["validation_error"]
            )
        )

        check_client_exists = rail.IfOperator(
            task_id='check_client_exists',
            test=lambda: rail.result("get_clients_in_replicon")["exists"],
            yes_task='get_client_details',
            no_task='create_client'
        )

        get_client_details = rail.RepliconServiceOperator(
            task_id='get_client_details',
            endpoint='/services/ClientService1.svc/BulkGetClientDetails',
            data=lambda dag_run: {
                "clientUris": [rail.result("get_clients_in_replicon")["client_uri"]]
            },
            data_handler=lambda response: response[0].get('code', '') if response and response[0] else ''
        )

        check_clientcode_matches = rail.IfOperator(
            task_id='check_clientcode_matches',
            test=lambda dag_run: rail.result("get_client_details") == dag_run.conf.get("client_id"),
            yes_task='log_client_skip',
            no_task='update_client_code'
        )

        log_client_skip = rail.WriteLogOperator(
            task_id='log_client_skip',
            log='{{ dag_run.conf.log }}',
            message='Client processing skipped - no changes needed',
            severity='Success',
            properties=lambda dag_run: custom_method.get_client_log_properties(
                dag_run,
                'Skip (Client)',
                'Exception',
                f'Client exists with matching code: {rail.result("get_client_details")}'
            )
        )

        update_client_code = rail.RepliconServiceOperator(
            task_id='update_client_code',
            endpoint='/services/ClientService1.svc/UpdateCode',
            data=lambda dag_run: {
                "clientUri": rail.result("get_clients_in_replicon")["client_uri"],
                "code": dag_run.conf.get("client_id")
            }
        )

        log_client_update_success = rail.WriteLogOperator(
            task_id='log_client_update_success',
            log='{{ dag_run.conf.log }}',
            message='Client processed successfully',
            severity='Success',
            properties=lambda dag_run: custom_method.get_client_log_properties(
                dag_run,
                'Update (Client)',
                'Success',
                'Client updated successfully'
            )
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.get_create_client_payload_peoplesoft
        )

        log_client_operation_success = rail.WriteLogOperator(
            task_id='log_client_operation_success',
            log='{{ dag_run.conf.log }}',
            message='Client processed successfully',
            severity='Success',
            properties=lambda dag_run: custom_method.get_client_log_properties(
                dag_run,
                'Add (Client)',
                'Success',
                'Client created successfully'
            )
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            message='{{ get_error_message() }}',
            severity='Error',
            properties=lambda dag_run: custom_method.get_client_log_properties(
                dag_run,
                'Client Processing',
                'Error',
                '{{ get_error_message() }}'
            )
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_clients_in_replicon

        get_clients_in_replicon >> validate_input_data

        validate_input_data >> rail.Label("Yes") >> check_client_exists
        validate_input_data >> rail.Label("No") >> log_validation_error

        check_client_exists >> rail.Label("Yes") >> get_client_details >> check_clientcode_matches
        check_client_exists >> rail.Label("No") >> create_client >> log_client_operation_success >> catch_and_log_errors

        check_clientcode_matches >> rail.Label("Yes") >> log_client_skip >> catch_and_log_errors
        check_clientcode_matches >> rail.Label("No") >> update_client_code >> log_client_update_success >> catch_and_log_errors

        log_validation_error >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_client_child_dag)