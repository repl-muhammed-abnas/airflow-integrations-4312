from tsystems.user_import_v2.utils import request_payload, custom_methods
from airflow.models import Variable
import rail

null = None

def create_supervisor_assignment_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.supervisor_assignment_child_dag_id,
        description="T-Systems Supervisor Assignment Child DAG - Assigns supervisors to users after all users are created",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.supervisor_assignment_child_max_active_runs
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")
        
        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="get_supervisor_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_supervisor_details",
            end_task="catch_and_log_errors"
        )

        # Get supervisor details - same pattern as update child DAG
        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf.get("supervisor"),
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        # Check if supervisor now exists in system
        if_supervisor_exists = rail.IfOperator(
            task_id="if_supervisor_exists",
            test='{{ result("get_supervisor_details") | is_truthy }}',
            yes_task="if_supervisor_permission_exists",
            no_task="log_supervisor_still_missing"
        )

        # Check if supervisor has required permission - same pattern as update child DAG
        if_supervisor_permission_exists = rail.IfOperator(
            task_id="if_supervisor_permission_exists",
            test=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_supervisor_details", {}).get("permissionSets", []),
                "displayText", config.defaults_mapper_data["supervisor_permission"], "uri"),
            yes_task="get_supervisor_assignment_details",
            no_task="assign_supervisor_permission"
        )

        # Assign supervisor permission if missing - same payload as update child DAG
        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: request_payload.get_assign_supervisor_permission_payload(config.defaults_mapper_data["supervisor_permission"])
        )

        # Get current supervisor assignment details - same service call as update child DAG
        get_supervisor_assignment_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_assignment_details",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("user_uri"),
                "asOfDate": rail.parse_date(dag_run.conf.get("current_date"), config.YMD_DATE_FORMAT)
            },
            data_handler=lambda response: response["supervisor"] if response else null
        )

        # Assign supervisor to user - same service call pattern as update child DAG
        assign_supervisor_to_user = rail.RepliconServiceOperator(
            task_id="assign_supervisor_to_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_supervisor_assignment_payload(dag_run, config.YMD_DATE_FORMAT)
        )

        # Log successful supervisor assignment
        log_supervisor_assignment_success = rail.WriteLogOperator(
            task_id="log_supervisor_assignment_success",
            log='{{ dag_run.conf.supervisor_assign_log }}',
            message="Supervisor assigned successfully",
            severity="Success",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "action": dag_run.conf["action"],
                "status": "Success",
                "details": "Supervisor assigned successfully"
            }
        )

        # Log when supervisor still missing
        log_supervisor_still_missing = rail.WriteLogOperator(
            task_id="log_supervisor_still_missing",
            log='{{ dag_run.conf.supervisor_assign_log }}',
            message="Supervisor is not available in Replicon",
            severity="Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "action": dag_run.conf["action"],
                "status": "Exception",
                "details": f'Supervisor {dag_run.conf.get("supervisor")} is not available in Replicon'
            }
        )

        # Error handler - same pattern as update child DAG
        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.supervisor_assign_log }}',
            message=lambda: "Supervisor assignment failed: " + custom_methods.get_error_message(),
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "action": dag_run.conf["action"],
                "status": "Error",
                "details": "Supervisor assignment failed: " + custom_methods.get_error_message()
            }
        )

        # Task Flow - same pattern as update child DAG
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_supervisor_details

        get_supervisor_details >> if_supervisor_exists

        if_supervisor_exists >> rail.Label("Yes") >> if_supervisor_permission_exists
        # When supervisor is missing, log_supervisor_still_missing logs the issue and succeeds.
        # catch_and_log_errors (trigger_rule="one_failed") is intentionally skipped since no task failed.
        if_supervisor_exists >> rail.Label("No") >> log_supervisor_still_missing >> catch_and_log_errors

        if_supervisor_permission_exists >> rail.Label("Yes") >> get_supervisor_assignment_details
        if_supervisor_permission_exists >> rail.Label("No") >> assign_supervisor_permission >> get_supervisor_assignment_details

        get_supervisor_assignment_details >> assign_supervisor_to_user >> log_supervisor_assignment_success >> catch_and_log_errors

    return dag

rail.for_each_instance(create_supervisor_assignment_child_dag)