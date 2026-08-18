from bearingpoint.user_import_v1.utils import request_payload
from bearingpoint.user_import_v1.tasks.process_user_groups_data import get_all_groups_data
from airflow.models import Variable
import rail
null = None


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.add_user_child_dag_id,
        description=f"BearingPoint User Import Add User Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.add_user_child_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="groups_data_start"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="groups_data_start",
            end_task="catch_and_log_errors"
        )

        groups_data_start = rail.EmptyOperator(
            task_id='groups_data_start'
        )

        process_get_groups_data = get_all_groups_data()

        if_user_and_supervisor_same = rail.IfOperator(
            task_id='if_user_and_supervisor_same',
            test='{{ dag_run.conf.employee_id == dag_run.conf.supervisor }}',
            yes_task='create_new_user',
            no_task='get_supervisor_details'
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["supervisor"],
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        if_supervisor_exists = rail.IfOperator(
            task_id="if_supervisor_exists",
            test='{{ result("get_supervisor_details") | is_truthy }}',
            yes_task="if_supervisor_permission_exists",
            no_task="create_new_user"
        )

        if_supervisor_permission_exists = rail.IfOperator(
            task_id="if_supervisor_permission_exists",
            test=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_supervisor_details")["permissionSets"],
                                                              "displayText", config.SUPERVISOR_PERMISSION, "uri"),
            yes_task="create_new_user",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: request_payload.get_assign_supervisor_permission_payload(config.SUPERVISOR_PERMISSION)
        )

        create_new_user = rail.RepliconServiceOperator(
            task_id="create_new_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_create_new_user_payload(
                dag_run, config)
        )

        if_user_created_with_errors = rail.IfOperator(
            task_id='if_user_created_with_errors',
            test=lambda: bool(rail.result("create_new_user")[
                              "errors"][0]["notifications"]) if rail.result("create_new_user")["errors"] else False,
            yes_task='write_added_user_with_exceptions_logs',
            no_task='write_added_user_logs'
        )

        write_added_user_with_exceptions_logs = rail.WriteLogOperator(
            task_id="write_added_user_with_exceptions_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda: "User partially created with errors - " +
                " | ".join([details["displayText"] for details in rail.result("create_new_user")["errors"][0]["notifications"]]),
            severity="Error",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": "Add",
                "status": "Error",
                "details": "User partially created with errors - " + " | ".join(
                    [details["displayText"] for details in rail.result("create_new_user")["errors"][0]["notifications"]])
            }
        )

        write_added_user_logs = rail.WriteLogOperator(
            task_id="write_added_user_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: "User created successfully" if not request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) else
                ("User partially created - " + " | ".join(request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper))),
            severity=lambda dag_run: "Success" if not request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) else "Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": "Add",
                "status": "Success" if not request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) else "Exception",
                "details": "User created successfully" if not request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) else ("User partially created - " + " | ".join(
                    request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper)))
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.log_artifact }}',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule="one_failed",
            properties={
                "employeeid": '{{ dag_run.conf.employee_id }}',
                "action": "Add",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> groups_data_start >> process_get_groups_data >> if_user_and_supervisor_same
        if_user_and_supervisor_same >> rail.Label("No") >> get_supervisor_details >> if_supervisor_exists
        if_user_and_supervisor_same >> rail.Label("Yes") >> create_new_user
        if_supervisor_exists >> rail.Label(
            "Yes") >> if_supervisor_permission_exists
        if_supervisor_exists >> rail.Label("No") >> create_new_user

        if_supervisor_permission_exists >> rail.Label("Yes") >> create_new_user
        if_supervisor_permission_exists >> rail.Label(
            "No") >> assign_supervisor_permission >> create_new_user >> if_user_created_with_errors

        if_user_created_with_errors >> rail.Label(
            "Yes") >> write_added_user_with_exceptions_logs >> catch_and_log_errors
        if_user_created_with_errors >> rail.Label(
            "No") >> write_added_user_logs >> catch_and_log_errors

        return dag


rail.for_each_instance(create_airflow_child)
