
from alvarezandmarsalholdings.user_import_v4.utils import request_payload, custom_methods
from airflow.models import Variable
import rail
null = None


def create_aiflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.assign_supervisor_dag_id,
        description="alvarezandmarsalholdings User import supervisor assignment",
        company_key=config.company_key,
        max_active_runs=config.max_active_run_child,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_location_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="validate_supervisor_employee_id_unequal"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="validate_supervisor_employee_id_unequal",
            end_task="catch_and_log_errors"
        )

        validate_supervisor_employee_id_unequal = rail.IfOperator(
            task_id="validate_supervisor_employee_id_unequal",
            test=lambda dag_run: (
                dag_run.conf["reporting_manager"] and
                dag_run.conf["reporting_manager"] != dag_run.conf["employee_id"]),
            yes_task="get_supervisor_user_details",
            no_task="write_supervisor_employee_id_match_log"
        )

        write_supervisor_employee_id_match_log = rail.WriteLogOperator(
            task_id="write_supervisor_employee_id_match_log",
            log='{{dag_run.conf.supervisor_log}}',
            message="Supervisor not assigned",
            severity="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "status": "Exception",
                "details": "Supervisor assignment skipped as Manager & employee Id are same"
            }
        )

        get_supervisor_user_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["reporting_manager"],
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_supervisor_in_replicon = rail.IfOperator(
            task_id="if_supervisor_in_replicon",
            test='{{result("get_supervisor_user_details")| is_truthy}}',
            yes_task="if_supervisor_has_permissions",
            no_task="log_exception_supervisor_not_present"
        )

        if_supervisor_has_permissions = rail.IfOperator(
            task_id="if_supervisor_has_permissions",
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_supervisor_user_details")[
                    0]["permissionSets"],
                "displayText",
                config.GENERAL_MAPPER["supervisor_permission"],
                "uri") and rail.find_first_by_attr_and_get_attr(
                rail.result("get_supervisor_user_details")[
                    0]["permissionSets"],
                "displayText",
                config.GENERAL_MAPPER["end_user_with_report_permission"],
                "uri"),
            yes_task="if_initial_supervisor",
            no_task="update_supervisor_permission"
        )

        log_exception_supervisor_not_present = rail.WriteLogOperator(
            task_id="log_exception_supervisor_not_present",
            log='{{dag_run.conf.supervisor_log}}',
            message="Supervisor not present",
            severity="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "status": "Exception",
                "details": "Supervisor not present in replicon"
            }
        )

        update_supervisor_permission = rail.RepliconServiceOperator(
            task_id="update_supervisor_permission",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_update_supervisor_permission_payload
        )

        write_log_supervisor_updated = rail.WriteLogOperator(
            task_id="write_log_supervisor_updated",
            log='{{dag_run.conf.supervisor_log}}',
            message="Supervisor permission updated",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "status": "Success",
                "details": "Supervisor permission updated ",
            }
        )

        if_initial_supervisor = rail.IfOperator(
            task_id="if_initial_supervisor",
            test=lambda dag_run: (
                (dag_run.conf["Add_Update"] == "Add" or not dag_run.conf.get("supervisor_uri")) and (dag_run.conf["type"] == 'reporting_manager')),
            yes_task="assign_initial_supervisor_to_employee",
            no_task="check_supervisor_update"
        )

        assign_initial_supervisor_to_employee = rail.RepliconServiceOperator(
            task_id="assign_initial_supervisor_to_employee",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result("get_supervisor_user_details")[0]["userDetails"]["uri"],
                "dateRange": null
            }
        )

        check_supervisor_update = rail.IfOperator(
            task_id="check_supervisor_update",
            test=custom_methods.check_supervisor_update,
            yes_task="assign_supervisor_to_employee",
            no_task="end_supervisor"
        )

        assign_supervisor_to_employee = rail.RepliconServiceOperator(
            task_id="assign_supervisor_to_employee",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result("get_supervisor_user_details")[0]["userDetails"]["uri"],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf['reporting_manager_effective_date'], custom_methods.DATE_FORMAT) if dag_run.conf['reporting_manager_effective_date'] else custom_methods.get_today_date()
                }
            }
        )

        write_log_supervisor_assignment_updated = rail.WriteLogOperator(
            task_id="write_log_supervisor_assignment_updated",
            log='{{dag_run.conf.supervisor_log}}',
            message="User updated",
            severity="Success",
            trigger_rule="all_success",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "status": "Success",
                "details": "Supervisor updated",
            }
        )

        end_supervisor = rail.EmptyOperator(task_id="end_supervisor")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.supervisor_log}}',
            message="Supervisor not processed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "status": "Failed",
                "details": "Supervisor not processed",
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            validate_supervisor_employee_id_unequal >> rail.Label("Yes") >>\
            write_supervisor_employee_id_match_log >> catch_and_log_errors
        validate_supervisor_employee_id_unequal >> rail.Label("No") >>\
            get_supervisor_user_details >>\
            if_supervisor_in_replicon >> rail.Label(
                "No") >> log_exception_supervisor_not_present >> end_supervisor
        if_supervisor_in_replicon >> rail.Label("Yes") >>\
            if_supervisor_has_permissions >> rail.Label("Yes") >>\
            if_initial_supervisor
        if_supervisor_has_permissions >> rail.Label("No") >>\
            update_supervisor_permission >>\
            write_log_supervisor_updated >> if_initial_supervisor
        if_initial_supervisor >> rail.Label(
            "Yes") >> assign_initial_supervisor_to_employee >> catch_and_log_errors
        if_initial_supervisor >> rail.Label("No") >> check_supervisor_update >>\
            rail.Label("Yes") >> assign_supervisor_to_employee >>\
            write_log_supervisor_assignment_updated >> end_supervisor
        check_supervisor_update >> rail.Label("No") >> end_supervisor >>\
            catch_and_log_errors
        return dag


rail.for_each_instance(create_aiflow_child)
