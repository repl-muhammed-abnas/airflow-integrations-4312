from datetime import timedelta
import rail
from airflow.models import Variable
from neology.user_import.utils import request_payload
null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_user_child_dagid,
        description=f'Neology BambooHR to Polaris User Sync Process Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_user_child_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_and_log_errors',
        )

        create_user_log = rail.CreateLogOperator(task_id="create_user_log")

        get_user_details_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_details_from_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: request_payload.get_user_details_from_replicon(dag_run.conf["user_details"]["employeenumber"]),
            data_handler=lambda response: response[0] if response else null
        )

        is_user_exists_in_replicon = rail.IfOperator(
            task_id='is_user_exists_in_replicon',
            test='{{ result("get_user_details_from_replicon") | is_truthy }}',
            yes_task='check_bamboohr_integration_field',
            no_task='trigger_create_user'
        )

        check_bamboohr_integration_field = rail.IfOperator(
            task_id='check_bamboohr_integration_field',
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_details_from_replicon")["userDetails"].get("extensionFieldValues", []),
                "definition.displayText", "BambooHR Integration", "textValue").lower() == "true",
            yes_task='trigger_update_user',
            no_task='log_user_bamboohr_integration_not_enabled'
        )

        log_user_bamboohr_integration_not_enabled = rail.WriteLogOperator(
            task_id='log_user_bamboohr_integration_not_enabled',
            log='{{result("create_user_log")}}',
            message="BambooHR Integration field is not set to True",
            severity="Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["user_details"]["employeenumber"],
                "action": "Validation",
                "status": "Exception",
                "details": "BambooHR Integration field is not set to True"
            }
        )

        trigger_update_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_update_user',
            items=["one"],
            trigger_dag_id=config.update_user_child_dagid,
            conf=lambda dag_run: {
                **dag_run.conf,
                "log_artifact": rail.result("create_user_log")
            }
        )

        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_update_user",
            dag_runs="{{result('trigger_update_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        trigger_create_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_create_user',
            items=["one"],
            trigger_dag_id=config.create_user_child_dagid,
            conf=lambda dag_run: {
                **dag_run.conf,
                "log_artifact": rail.result("create_user_log")
            }
        )

        wait_for_create_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_create_user",
            dag_runs="{{result('trigger_create_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{result('create_user_log')}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "employeeid": '{{ dag_run.conf.user_details.employeenumber }}',
                "action": "Process User",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_user_log

        create_user_log >> get_user_details_from_replicon >> is_user_exists_in_replicon

        is_user_exists_in_replicon >> rail.Label("Yes") >> check_bamboohr_integration_field
        check_bamboohr_integration_field >> rail.Label("Yes") >> trigger_update_user >> wait_for_update_user >> catch_and_log_errors
        check_bamboohr_integration_field >> rail.Label("No") >> log_user_bamboohr_integration_not_enabled >> catch_and_log_errors
        is_user_exists_in_replicon >> rail.Label("No") >> trigger_create_user >> wait_for_create_user >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
