from tsystems.user_import_v1.utils import request_payload, custom_methods
from airflow.models import Variable
import rail

null = None
false = False

def create_add_user_child_dag(config):
    append_dags = []
    for idx in range(0, config.PROCESS_USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f'{config.add_user_child_dag_id}_batch_{idx+1}',
            description="T-Systems Add User Child DAG - Creates new users in Replicon",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.add_user_child_max_active_runs
        ) as dag:
            
            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id="can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var="true").lower() == "true",
                yes_task="batch_task",
                no_task="if_user_and_supervisor_same"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id="batch_task",
                start_task="if_user_and_supervisor_same",
                end_task="catch_and_log_errors"
            )

            if_user_and_supervisor_same = rail.IfOperator(
                task_id='if_user_and_supervisor_same',
                test='{{ dag_run.conf.employeeid == dag_run.conf.supervisorempid }}',
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
                            "employeeId": dag_run.conf["supervisorempid"],
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
                no_task="if_supervisor_present_in_payload"
            )

            if_supervisor_present_in_payload = rail.IfOperator(
                task_id='if_supervisor_present_in_payload',
                test=lambda dag_run: dag_run.conf.get("supervisorempid") in custom_methods.get_all_user_employee_ids_from_feed(dag_run),
                yes_task='pending_supervisor_flag',
                no_task='create_new_user'
            )

            pending_supervisor_flag = rail.SetVariableOperator(
                task_id='pending_supervisor_flag',
                name='pending_supervisor_flag',
                value='true'
            )

            if_supervisor_permission_exists = rail.IfOperator(
                task_id="if_supervisor_permission_exists",
                test=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_supervisor_details")["permissionSets"],
                    "displayText", config.defaults_mapper_data["supervisor_permission"], "uri"),
                yes_task="create_new_user",
                no_task="assign_supervisor_permission"
            )

            assign_supervisor_permission = rail.RepliconServiceOperator(
                task_id="assign_supervisor_permission",
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=lambda: request_payload.get_assign_supervisor_permission_payload(config.defaults_mapper_data["supervisor_permission"])
            )

            create_new_user = rail.RepliconServiceOperator(
                task_id="create_new_user",
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=lambda dag_run: request_payload.get_user_creation_payload(
                    dag_run, config)
            )

            if_user_created_with_errors = rail.IfOperator(
                task_id='if_user_created_with_errors',
                test=lambda: bool(rail.result("create_new_user")[
                                "errors"][0]["notifications"]) if rail.result("create_new_user")["errors"] else false,
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
                    "employeeid": dag_run.conf["employeeid"],
                    "action": "Add",
                    "status": "Error",
                    "details": "User partially created with errors - " + " | ".join(
                        [details["displayText"] for details in rail.result("create_new_user")["errors"][0]["notifications"]])
                }
            )

            write_added_user_logs = rail.WriteLogOperator(
                task_id="write_added_user_logs",
                log='{{ dag_run.conf.log_artifact }}',
                message=lambda dag_run: "User created successfully" if not request_payload.get_exception_logs(dag_run, false, config) else
                    ("User partially created - " + " | ".join(request_payload.get_exception_logs(dag_run, false, config))),
                severity=lambda dag_run: "Success" if not request_payload.get_exception_logs(dag_run, false, config) else "Exception",
                properties=lambda dag_run: {
                    "employeeid": dag_run.conf["employeeid"],
                    "action": "Add",
                    "status": "Success" if not request_payload.get_exception_logs(dag_run, false, config) else "Exception",
                    "details": "User created successfully" if not request_payload.get_exception_logs(dag_run, false, config) else ("User partially created - " + " | ".join(
                        request_payload.get_exception_logs(dag_run, false, config)))
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
                no_task='finish_user_creation'
            )

            write_supervisor_pending_logs = rail.WriteLogOperator(
                task_id="write_supervisor_pending_logs",
                log='{{ dag_run.conf.supervisor_log }}',
                message="Supervisor assignment pending",
                severity="Pending",
                properties=lambda dag_run: {
                    "employeeid": dag_run.conf["employeeid"],
                    "supervisor": dag_run.conf["supervisorempid"],
                    "action": "Add",
                    "user_uri": rail.result("create_new_user", {}).get("user", {}).get("uri"),
                }
            )

            finish_user_creation = rail.EmptyOperator(
                task_id='finish_user_creation'
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id="catch_and_log_errors",
                log='{{ dag_run.conf.log_artifact }}',
                message='{{ get_error_message() }}',
                severity="Error",
                trigger_rule="one_failed",
                properties={
                    "employeeid": '{{ dag_run.conf.employeeid }}',
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
            if_user_and_supervisor_same >> rail.Label("Yes") >> create_new_user >> if_user_created_with_errors
            if_supervisor_exists >> rail.Label(
                "Yes") >> if_supervisor_permission_exists
            if_supervisor_exists >> rail.Label("No") >> if_supervisor_present_in_payload

            if_supervisor_permission_exists >> rail.Label("Yes") >> create_new_user >> if_user_created_with_errors
            if_supervisor_permission_exists >> rail.Label(
                "No") >> assign_supervisor_permission >> create_new_user >> if_user_created_with_errors
            if_supervisor_present_in_payload >> rail.Label("Yes") >> pending_supervisor_flag >> create_new_user >> if_user_created_with_errors
            if_supervisor_present_in_payload >> rail.Label("No") >> create_new_user >> if_user_created_with_errors
            
            if_user_created_with_errors >> rail.Label("Yes") >> write_added_user_with_exceptions_logs >> get_pending_supervisor_flag >> if_pending_supervisor_flag
            if_user_created_with_errors >> rail.Label("No") >> write_added_user_logs >> get_pending_supervisor_flag >> if_pending_supervisor_flag
            
            if_pending_supervisor_flag >> rail.Label("Yes") >> write_supervisor_pending_logs >> finish_user_creation
            if_pending_supervisor_flag >> rail.Label("No") >> finish_user_creation

            finish_user_creation >> catch_and_log_errors

        append_dags.append(dag)
    return append_dags

# Create child DAG for each instance
rail.for_each_instance(create_add_user_child_dag)