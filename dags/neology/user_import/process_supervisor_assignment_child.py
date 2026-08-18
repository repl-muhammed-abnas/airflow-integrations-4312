from neology.user_import.utils import request_payload, custom_methods
from airflow.models import Variable
import rail

null = None

def create_supervisor_assignment_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.supervisor_assignment_child_dag_id,
        description=f"Neology Supervisor Assignment Child DAG {config.instance} - Assigns supervisors to users after all users are created",
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

        # Get supervisor details using employee ID
        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_user_details_from_replicon(dag_run.conf["supervisor_id"]),
            data_handler=lambda response: response[0] if response else null
        )

        # Check if supervisor now exists in system
        if_supervisor_exists = rail.IfOperator(
            task_id="if_supervisor_exists",
            test='{{ result("get_supervisor_details") | is_truthy }}',
            yes_task="if_supervisor_permission_exists",
            no_task="log_supervisor_still_missing"
        )

        # Check if supervisor has required permission
        if_supervisor_permission_exists = rail.IfOperator(
            task_id="if_supervisor_permission_exists",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                rail.result("get_supervisor_details")["permissionSets"],
                    "displayText", config.supervisor_permission_set[0], "uri")),
            yes_task="get_supervisor_assignment_details",
            no_task="assign_supervisor_permission"
        )

        # Assign supervisor permission if missing
        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: request_payload.get_assign_supervisor_permission_payload(config.supervisor_permission_set[0])
        )

        # Get current supervisor assignment details
        get_supervisor_assignment_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_assignment_details",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("user_uri"),
                "asOfDate": dag_run.conf["process_start_time"]
            },
            data_handler=lambda response: response["supervisor"] if response else null
        )

        # Assign supervisor to user
        assign_supervisor_to_user = rail.RepliconServiceOperator(
            task_id="assign_supervisor_to_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.get_supervisor_assignment_payload
        )

        # Log successful supervisor assignment
        log_supervisor_assignment_success = rail.WriteLogOperator(
            task_id="log_supervisor_assignment_success",
            log='{{ dag_run.conf.supervisor_assign_log }}',
            message="Supervisor assigned successfully",
            severity="Success",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": dag_run.conf["action"],
                "status": "Success",
                "details": f"Supervisor {dag_run.conf['supervisor_id']} assigned successfully"
            }
        )

        # Log when supervisor still missing
        log_supervisor_still_missing = rail.WriteLogOperator(
            task_id="log_supervisor_still_missing",
            log='{{ dag_run.conf.supervisor_assign_log }}',
            message="Supervisor is not available in Replicon",
            severity="Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": dag_run.conf["action"],
                "status": "Exception",
                "details": f'Supervisor {dag_run.conf.get("supervisor_id")} is not available in Replicon'
            }
        )

        # Error handler
        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.supervisor_assign_log }}',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule="one_failed",
            properties={
                "employeeid": '{{ dag_run.conf.employee_id }}',
                "action": '{{ dag_run.conf.action }}',
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        # Task Flow
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_supervisor_details

        get_supervisor_details >> if_supervisor_exists

        if_supervisor_exists >> rail.Label("Yes") >> if_supervisor_permission_exists
        if_supervisor_exists >> rail.Label("No") >> log_supervisor_still_missing >> catch_and_log_errors

        if_supervisor_permission_exists >> rail.Label("Yes") >> get_supervisor_assignment_details
        if_supervisor_permission_exists >> rail.Label("No") >> assign_supervisor_permission >> get_supervisor_assignment_details

        get_supervisor_assignment_details >> assign_supervisor_to_user >> log_supervisor_assignment_success >> catch_and_log_errors

    return dag

rail.for_each_instance(create_supervisor_assignment_child_dag)