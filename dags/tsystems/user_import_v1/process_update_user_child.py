from tsystems.user_import_v1.utils import request_payload, response_filters, custom_methods
from airflow.models import Variable
import rail

null = None
true = True
false = False

def create_update_user_child_dag(config):
    append_dags = []
    for idx in range(0, config.PROCESS_USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f'{config.update_user_child_dag_id}_batch_{idx+1}',
            description="T-Systems Update User Child DAG - Updates existing users in Replicon",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.update_user_child_max_active_runs
        ) as dag:
            
            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")
            
            can_run_batch_task = rail.IfOperator(
                task_id="can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var="true").lower() == "true",
                yes_task="batch_task",
                no_task="get_user_details"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id="batch_task",
                start_task="get_user_details",
                end_task="catch_and_log_errors"
            )

            get_user_details = rail.RepliconServiceOperator(
                task_id="get_user_details",
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=lambda dag_run: {
                    "users": [
                        {
                            "uri": null,
                            "loginName": null,
                            "employeeId": dag_run.conf["employeeid"],
                            "parameterCorrelationId": null
                        }
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                },
                data_handler=lambda response: response[0] if response else null
            )

            get_current_group_membership = rail.RepliconServiceOperator(
                task_id="get_current_group_membership",
                endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
                data=lambda dag_run: {
                        "userUri": rail.result("get_user_details")["userDetails"]['uri'],
                        "dateRange": {
                            "startDate": rail.parse_date(dag_run.conf["current_date"], config.YMD_DATE_FORMAT),
                            "endDate": rail.parse_date(dag_run.conf["current_date"], config.YMD_DATE_FORMAT)
                        }
                },
                data_handler=response_filters.get_current_group_membership
            )

            get_user_holiday_calendar = rail.RepliconServiceOperator(
                task_id='get_user_holiday_calendar',
                endpoint="/services/HolidayCalendarService2.svc/GetHolidayCalendarAssignmentScheduleForUserAndDateRange",
                data=lambda dag_run: request_payload.get_user_holiday_cal_payload(dag_run, config.YMD_DATE_FORMAT),
                data_handler=response_filters.get_user_current_holiday_calendar
            )

            if_user_and_supervisor_same = rail.IfOperator(
                task_id='if_user_and_supervisor_same',
                test='{{ dag_run.conf.employeeid == dag_run.conf.supervisorempid }}',
                yes_task='get_update_user_payload',
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
                yes_task='write_supervisor_pending_logs',
                no_task='get_update_user_payload'
            )

            write_supervisor_pending_logs = rail.WriteLogOperator(
                task_id="write_supervisor_pending_logs",
                log='{{ dag_run.conf.supervisor_log }}',
                message="Supervisor assignment pending",
                severity="Pending",
                properties=lambda dag_run: {
                    "employeeid": dag_run.conf["employeeid"],
                    "supervisor": dag_run.conf["supervisorempid"],
                    "action": "Update",
                    "user_uri": rail.result("get_user_details")["userDetails"]['uri']
                }
            )

            if_supervisor_permission_exists = rail.IfOperator(
                task_id="if_supervisor_permission_exists",
                test=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_supervisor_details")["permissionSets"],
                    "displayText", config.defaults_mapper_data["supervisor_permission"], "uri"),
                yes_task="get_supervisor_assignment_details",
                no_task="assign_supervisor_permission"
            )

            assign_supervisor_permission = rail.RepliconServiceOperator(
                task_id="assign_supervisor_permission",
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=lambda: request_payload.get_assign_supervisor_permission_payload(config.defaults_mapper_data["supervisor_permission"])
            )

            get_supervisor_assignment_details = rail.RepliconServiceOperator(
                task_id="get_supervisor_assignment_details",
                endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
                data=lambda dag_run: {
                    "userUri": rail.result("get_user_details")["userDetails"]["uri"],
                    "asOfDate": rail.parse_date(dag_run.conf["current_date"], config.YMD_DATE_FORMAT)
                },
                data_handler=lambda response: response["supervisor"] if response else null
            )

            get_update_user_payload = rail.PythonOperator(
                task_id="get_update_user_payload",
                python_callable=lambda dag_run: request_payload.get_update_user_req(dag_run, config.oef_field_mapper_data,
                    config.timesheet_template_mapper_data, config.employee_type_mapper_data, config.permissions_mapper_data,
                    config.custom_field_mapper_data, config.YMD_DATE_FORMAT, config.REP_DATE_FORMAT),
            )

            if_update_user_payload_exists = rail.IfOperator(
                task_id="if_update_user_payload_exists",
                test=lambda: bool(rail.result("get_update_user_payload")),
                yes_task="update_user_details",
                no_task="write_updated_user_logs"
            )

            update_user_details = rail.RepliconServiceOperator(
                task_id="update_user_details",
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=lambda: rail.result("get_update_user_payload"),
            )

            if_timeoff_types_need_update = rail.IfOperator(
                task_id="if_timeoff_types_need_update",
                test=lambda dag_run: bool(request_payload.get_updated_timeoff_types(dag_run).get("all_timeoff_types", [])),
                yes_task="assign_timeoff_types_to_user",
                no_task="if_user_updated_with_exceptions"
            )

            assign_timeoff_types_to_user = rail.RepliconServiceOperator(
                task_id="assign_timeoff_types_to_user",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=lambda dag_run: {
                    "userUri": rail.result("get_user_details")["userDetails"]["uri"],
                    "timeOffTypeUris": request_payload.get_updated_timeoff_types(dag_run).get("all_timeoff_types", [])
                }
            )

            if_user_updated_with_exceptions = rail.IfOperator(
                task_id='if_user_updated_with_exceptions',
                test=lambda: bool(rail.result("update_user_details")[
                                "errors"][0]["notifications"]) if rail.result("update_user_details")["errors"] else false,
                yes_task='write_updated_user_with_exceptions_logs',
                no_task='write_updated_user_logs'
            )

            write_updated_user_with_exceptions_logs = rail.WriteLogOperator(
                task_id="write_updated_user_with_exceptions_logs",
                log='{{ dag_run.conf.log_artifact }}',
                message=lambda dag_run: "User partially updated with errors - " + " | ".join(
                    request_payload.get_updated_logs(dag_run, config) + [details["displayText"]
                        for details in rail.result("update_user_details")["errors"][0]["notifications"]]
                            + request_payload.get_exception_logs(dag_run, true, config)),
                severity="Error",
                properties=lambda dag_run: {
                    "employeeid": dag_run.conf["employeeid"],
                    "action": request_payload.get_action_type(dag_run, config),
                    "status": "Error",
                    "details": "User partially updated with errors - " + " | ".join(request_payload.get_updated_logs(dag_run, config) + 
                        [details["displayText"] for details in rail.result("update_user_details")["errors"][0]["notifications"]]
                            + request_payload.get_exception_logs(dag_run, true, config))
                }
            )

            write_updated_user_logs = rail.WriteLogOperator(
                task_id="write_updated_user_logs",
                log='{{ dag_run.conf.log_artifact }}',
                message=lambda dag_run: "User updated successfully" if not request_payload.get_exception_logs(dag_run, true, config) and
                        request_payload.get_updated_logs(dag_run, config) else
                            ("User partially updated - " + " | ".join(request_payload.get_updated_logs(dag_run, config)
                            + request_payload.get_exception_logs(dag_run, true, config))) if request_payload.get_updated_logs(dag_run, config)
                                else ("User not updated - " + " | ".join(request_payload.get_exception_logs(dag_run, true, config))
                                    if request_payload.get_exception_logs(dag_run, true, config) else "User not updated"),
                severity=lambda dag_run: ("Success" if not request_payload.get_exception_logs(dag_run, true, config) and
                        request_payload.get_updated_logs(dag_run, config) else ("Exception" if request_payload.get_exception_logs(
                            dag_run, true, config) else "Skipped")),
                properties=lambda dag_run: {
                    "employeeid": dag_run.conf["employeeid"],
                    "action": request_payload.get_action_type(dag_run, config),
                    "status": ("Success" if not request_payload.get_exception_logs(dag_run, true, config) and
                        request_payload.get_updated_logs(dag_run, config) else ("Exception" if request_payload.get_exception_logs(
                            dag_run, true, config) else "Skipped")),
                    "details": "User updated successfully" if not request_payload.get_exception_logs(dag_run, true, config) and
                        request_payload.get_updated_logs(dag_run, config) else
                            ("User partially updated - " + " | ".join(request_payload.get_updated_logs(dag_run, config)
                            + request_payload.get_exception_logs(dag_run, true, config))) if request_payload.get_updated_logs(dag_run, config)
                                else ("User not updated - " + " | ".join(request_payload.get_exception_logs(dag_run, true, config))
                                    if request_payload.get_exception_logs(dag_run, true, config) else "User not updated"),
                }
            )

            finish_user_update = rail.EmptyOperator(
                task_id='finish_user_update'
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id="catch_and_log_errors",
                log='{{ dag_run.conf.log_artifact }}',
                message='{{ get_error_message() }}',
                severity="Error",
                trigger_rule="one_failed",
                properties=lambda dag_run: {
                    "employeeid": dag_run.conf["employeeid"],
                    "action": request_payload.get_action_type(dag_run, config),
                    "status": "Error",
                    "details": rail.render_template("{{ get_error_message() }}")
                }
            )

            can_run_batch_task >> rail.Label(
                "Yes") >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label("No") >> get_user_details >> get_current_group_membership \
                >> get_user_holiday_calendar >> if_user_and_supervisor_same >> rail.Label("No") >> get_supervisor_details \
                        >> if_supervisor_exists >> rail.Label("Yes") >> if_supervisor_permission_exists
            if_user_and_supervisor_same >> rail.Label("Yes") >> get_update_user_payload
            if_supervisor_exists >> rail.Label("No") >> if_supervisor_present_in_payload
            if_supervisor_permission_exists >> rail.Label(
                "No") >> assign_supervisor_permission >> get_supervisor_assignment_details
            if_supervisor_permission_exists >> rail.Label("Yes") >> get_supervisor_assignment_details \
                >> get_update_user_payload >> if_update_user_payload_exists
            if_supervisor_present_in_payload >> rail.Label("Yes") >> write_supervisor_pending_logs >> get_update_user_payload
            if_supervisor_present_in_payload >> rail.Label("No") >> get_update_user_payload
            if_update_user_payload_exists >> rail.Label("Yes") >> update_user_details >> if_timeoff_types_need_update
            if_update_user_payload_exists >> rail.Label("No") >> write_updated_user_logs
            if_timeoff_types_need_update >> rail.Label("Yes") >> assign_timeoff_types_to_user >> if_user_updated_with_exceptions
            if_timeoff_types_need_update >> rail.Label("No") >> if_user_updated_with_exceptions
            if_user_updated_with_exceptions >> rail.Label(
                "No") >> write_updated_user_logs >> finish_user_update
            if_user_updated_with_exceptions >> rail.Label(
                "Yes") >> write_updated_user_with_exceptions_logs >> finish_user_update
            
            finish_user_update >> catch_and_log_errors

        append_dags.append(dag)
    return append_dags

# Create child DAG for each instance
rail.for_each_instance(create_update_user_child_dag)