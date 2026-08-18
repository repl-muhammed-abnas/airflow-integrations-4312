from datetime import timedelta
from ipipeline.user_import.utils import custom_methods, request_payload
from airflow.models import Variable
import rail
null = None
true = True
false = False

def create_process_each_user_child(config):
    with rail.create_airflow_dag(
        dag_id=config.process_user_record_child_dag_id,
        description=f"iPipeline User Import Process User Record Child {config.instance}",
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
            no_task="user_child_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="user_child_log",
            end_task="catch_and_log_errors"
        )

        user_child_log = rail.CreateLogOperator(
            task_id='user_child_log'
        )

        if_employee_id_present = rail.IfOperator(
            task_id="if_employee_id_present",
            test=lambda dag_run: bool(dag_run.conf.get("employee_id")),
            yes_task="get_user_details",
            no_task="log_employee_id_missing"
        )

        log_employee_id_missing = rail.WriteLogOperator(
            task_id="log_employee_id_missing",
            log='{{ result("user_child_log") }}',
            message="Employee ID is missing in the input data",
            severity="Exception",
            properties=lambda: {
                "employeeid": null,
                "action": "Validation", 
                "status": "Exception",
                "details": "Employee ID is missing in the input data"
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": dag_run.conf["login_name"],
                        "employeeId": null,
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
            yes_task="get_invalid_update_user_input_details",
            no_task="get_invalid_new_user_input_details"
        )

        get_invalid_update_user_input_details = rail.PythonOperator(
            task_id="get_invalid_update_user_input_details",
            python_callable=lambda dag_run: custom_methods.get_invalid_user_input_details(dag_run, true, config.input_fields_mapper_data)
        )

        if_missing_update_mandatory_fields = rail.IfOperator(
            task_id="if_missing_update_mandatory_fields",
            test=lambda: rail.result("get_invalid_update_user_input_details"),
            yes_task="log_missing_update_mandatory_fields",
            no_task="process_update_user"
        )

        log_missing_update_mandatory_fields = rail.WriteLogOperator(
            task_id="log_missing_update_mandatory_fields",
            log='{{ result("user_child_log") }}',
            message='{{ result("get_invalid_update_user_input_details") }}',
            severity="Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf.get("employee_id", ""),
                "action": "Validation",
                "status": "Exception",
                "details": rail.result("get_invalid_update_user_input_details")
            }
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id="process_update_user",
            trigger_dag_id=config.update_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:
            {
                **dag_run.conf,
                "log_artifact": rail.result("user_child_log")
            }
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("process_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_invalid_new_user_input_details = rail.PythonOperator(
            task_id="get_invalid_new_user_input_details",
            python_callable=lambda dag_run: custom_methods.get_invalid_user_input_details(dag_run, false, config.input_fields_mapper_data)
        )

        if_missing_add_user_mandatory_fields = rail.IfOperator(
            task_id="if_missing_add_user_mandatory_fields",
            test=lambda: rail.result("get_invalid_new_user_input_details"),
            yes_task="log_missing_add_user_mandatory_fields",
            no_task="process_add_user"
        )

        log_missing_add_user_mandatory_fields = rail.WriteLogOperator(
            task_id="log_missing_add_user_mandatory_fields",
            log='{{ result("user_child_log") }}',
            message='{{ result("get_invalid_new_user_input_details") }}',
            severity="Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf.get("employee_id", ""),
                "action": "Validation",
                "status": "Exception",
                "details": rail.result("get_invalid_new_user_input_details")
            }
        )

        process_add_user = rail.TriggerDagRunOperator(
            task_id="process_add_user",
            trigger_dag_id=config.add_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:
            {
                **dag_run.conf,
                "log_artifact": rail.result("user_child_log")
            }
        )

        wait_for_process_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_user',
            dag_runs='{{ result("process_add_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ result("user_child_log") }}',
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
        can_run_batch_task >> rail.Label("No") >> user_child_log

        user_child_log >> if_employee_id_present
        if_employee_id_present >> rail.Label("Yes") >> get_user_details >> if_user_exists
        if_employee_id_present >> rail.Label("No") >> log_employee_id_missing >> catch_and_log_errors
        if_user_exists >> rail.Label("Yes") >> get_invalid_update_user_input_details >> if_missing_update_mandatory_fields
        if_missing_update_mandatory_fields >> rail.Label("Yes") >> log_missing_update_mandatory_fields >> catch_and_log_errors
        if_missing_update_mandatory_fields >> rail.Label("No") >> process_update_user >> wait_for_process_update_user >> catch_and_log_errors
        if_user_exists >> rail.Label("No") >> get_invalid_new_user_input_details >> if_missing_add_user_mandatory_fields
        if_missing_add_user_mandatory_fields >> rail.Label("Yes") >> log_missing_add_user_mandatory_fields >> catch_and_log_errors
        if_missing_add_user_mandatory_fields >> rail.Label("No") >> process_add_user >> wait_for_process_add_user >> catch_and_log_errors

        return dag

rail.for_each_instance(create_process_each_user_child)