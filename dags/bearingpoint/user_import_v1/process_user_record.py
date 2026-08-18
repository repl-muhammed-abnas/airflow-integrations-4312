from datetime import timedelta
from bearingpoint.user_import_v1.utils import custom_methods
from airflow.models import Variable
import rail
null = None


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.process_user_record_child_dag_id,
        description=f"BearingPoint User Import Process User Record Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.process_user_child_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_log",
            end_task="catch_and_log_errors"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["employee_id"],
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        if_user_exists = rail.IfOperator(
            task_id="if_user_exists",
            test='{{ result("get_user_details") | is_truthy }}',
            yes_task="process_update_user",
            no_task="process_add_user"
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id="process_update_user",
            trigger_dag_id=config.update_user_child_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda dag_run:
            {
                **dag_run.conf,
                "log_artifact": rail.result("create_log")
            }
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("process_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        process_add_user = rail.TriggerDagRunOperator(
            task_id="process_add_user",
            trigger_dag_id=config.add_user_child_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda dag_run:
            {
                **dag_run.conf,
                "log_artifact": rail.result("create_log")
            }
        )

        wait_for_process_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_user',
            dag_runs='{{ result("process_add_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ result("create_log") }}',
            message=lambda: "User not processed for the following reason/s " + custom_methods.get_error_message(),
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": "",
                "status": "Error",
                "details": "User not processed for the following reason/s " + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_log

        create_log >> get_user_details >> if_user_exists >> rail.Label("Yes") >> process_update_user \
            >> wait_for_process_update_user >> catch_and_log_errors
        if_user_exists >> rail.Label(
            "No") >> process_add_user >> wait_for_process_add_user >> catch_and_log_errors

        return dag


rail.for_each_instance(create_airflow_child)
