from datetime import timedelta
from neology.user_import.utils import request_payload, custom_methods
import rail
from airflow.models import Variable
null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_user_child_dagid,
        description=f'Neology BambooHR to Polaris User Sync Create Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.create_user_child_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_user_and_supervisor_same'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_user_and_supervisor_same',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        if_user_and_supervisor_same = rail.IfOperator(
            task_id='if_user_and_supervisor_same',
            test='{{ dag_run.conf.user_details.employeenumber == dag_run.conf.user_details.supervisorid }}',
            yes_task='create_user_in_replicon',
            no_task='get_supervisor_details'
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_user_details_from_replicon(dag_run.conf["user_details"]["supervisorid"]),
            data_handler=lambda response: response[0] if response else null
        )

        if_supervisor_exists = rail.IfOperator(
            task_id="if_supervisor_exists",
            test='{{ result("get_supervisor_details") | is_truthy }}',
            yes_task="if_supervisor_permission_exists",
            no_task="if_supervisor_present_in_payload"
        )

        if_supervisor_present_in_payload = rail.IfOperator(
            task_id='if_supervisor_present_in_payload',
            test=lambda dag_run: dag_run.conf.get("user_details", {}).get("supervisorid") in custom_methods.get_all_employee_numbers_from_payload(dag_run),
            yes_task='pending_supervisor_flag',
            no_task='create_user_in_replicon'
        )

        pending_supervisor_flag = rail.SetVariableOperator(
            task_id='pending_supervisor_flag',
            name='pending_supervisor_flag',
            value='true'
        )

        if_supervisor_permission_exists = rail.IfOperator(
            task_id="if_supervisor_permission_exists",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                rail.result("get_supervisor_details")["permissionSets"],
                    "displayText", config.supervisor_permission_set[0], "uri")),
            yes_task="create_user_in_replicon",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: request_payload.get_assign_supervisor_permission_payload(config.supervisor_permission_set[0])
        )
 
        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id='create_user_in_replicon',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_create_user_payload(dag_run,
                config.user_permission_set, config.supervisor_permission_set, config.replicon_default_password,
                    config.all_license_types, config.licenses, config.all_notifications)
        )

        if_user_created_with_errors = rail.IfOperator(
            task_id='if_user_created_with_errors',
            test=lambda: bool(rail.result("create_user_in_replicon")[
                "errors"][0]["notifications"]) if rail.result("create_user_in_replicon")["errors"] else False,
            yes_task='write_added_user_with_exceptions_logs',
            no_task='write_added_user_logs'
        )

        write_added_user_with_exceptions_logs = rail.WriteLogOperator(
            task_id="write_added_user_with_exceptions_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda: "User partially created with errors - " +
                " | ".join([details["displayText"] for details in rail.result("create_user_in_replicon")["errors"][0]["notifications"]]),
            severity="Error",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["user_details"]["employeenumber"],
                "action": "Add",
                "status": "Error",
                "details": "User partially created with errors - " + " | ".join(
                    [details["displayText"] for details in rail.result("create_user_in_replicon")["errors"][0]["notifications"]])
            }
        )

        write_added_user_logs = rail.WriteLogOperator(
            task_id="write_added_user_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: "User created successfully" if not request_payload.get_exception_logs(dag_run) else
                ("User partially created - " + " | ".join(request_payload.get_exception_logs(dag_run))),
            severity=lambda dag_run: "Success" if not request_payload.get_exception_logs(dag_run) else "Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["user_details"]["employeenumber"],
                "action": "Add",
                "status": "Success" if not request_payload.get_exception_logs(dag_run) else "Exception",
                "details": "User created successfully" if not request_payload.get_exception_logs(
                    dag_run) else ("User partially created - " + " | ".join(
                        request_payload.get_exception_logs(dag_run)))
            }
        )

        get_pending_supervisor_flag = rail.GetVariableOperator(
            task_id='get_pending_supervisor_flag',
            name='pending_supervisor_flag'
        )

        if_pending_supervisor_flag = rail.IfOperator(
            task_id='if_pending_supervisor_flag',
            test=lambda: rail.result("get_pending_supervisor_flag", {}).get("value") == 'true',
            yes_task='write_supervisor_pending_logs',
            no_task='catch_and_log_errors'
        )

        write_supervisor_pending_logs = rail.WriteLogOperator(
            task_id="write_supervisor_pending_logs",
            log='{{ dag_run.conf.supervisor_pending_log }}',
            message="Supervisor pending",
            severity="Pending",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["user_details"]["employeenumber"],
                "supervisor_id": dag_run.conf["user_details"]["supervisorid"],
                "action": "Add",
                "user_uri": rail.result("create_user_in_replicon")["user"]["uri"],
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.log_artifact }}',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule="one_failed",
            properties={
                "employeeid": '{{ dag_run.conf.user_details.employeenumber }}',
                "action": "Add",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> if_user_and_supervisor_same
        if_user_and_supervisor_same >> rail.Label("No") >> get_supervisor_details >> if_supervisor_exists
        if_user_and_supervisor_same >> rail.Label("Yes") >> create_user_in_replicon
        if_supervisor_exists >> rail.Label(
            "Yes") >> if_supervisor_permission_exists
        if_supervisor_exists >> rail.Label("No") >> if_supervisor_present_in_payload
        if_supervisor_present_in_payload >> rail.Label("Yes") >> pending_supervisor_flag >> create_user_in_replicon
        if_supervisor_present_in_payload >> rail.Label("No") >> create_user_in_replicon

        if_supervisor_permission_exists >> rail.Label("Yes") >> create_user_in_replicon
        if_supervisor_permission_exists >> rail.Label(
            "No") >> assign_supervisor_permission >> create_user_in_replicon
        create_user_in_replicon >> if_user_created_with_errors

        if_user_created_with_errors >> rail.Label(
            "Yes") >> write_added_user_with_exceptions_logs >> catch_and_log_errors
        if_user_created_with_errors >> rail.Label(
            "No") >> write_added_user_logs >> get_pending_supervisor_flag >> if_pending_supervisor_flag
        if_pending_supervisor_flag >> rail.Label("Yes") >> write_supervisor_pending_logs >> catch_and_log_errors
        if_pending_supervisor_flag >> rail.Label("No") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
