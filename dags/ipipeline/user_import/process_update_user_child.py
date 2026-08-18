import json
from ipipeline.user_import.utils import request_payload, response_filters, custom_methods
from airflow.models import Variable
import rail

null = None

def create_update_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_child_dag_id,
        description="iPipeline Update User Child DAG - Updates existing users in Replicon",
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
                        "loginName": dag_run.conf["login_name"],
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        if_user_exists_in_replicon = rail.IfOperator(
            task_id='if_user_exists_in_replicon',
            test=lambda: bool(rail.result('get_user_details')),
            yes_task='get_notification_preferences_for_user',
            no_task='log_user_not_present_in_replicon'
        )

        log_user_not_present_in_replicon = rail.WriteLogOperator(
            task_id='log_user_not_present_in_replicon',
            log='{{ dag_run.conf.log_artifact }}',
            message="User not present in Replicon",
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employee_id'],
                "action": "Update",
                "status": "Exception",
                'details': "User not present in Replicon"
            }
        )

        get_notification_preferences_for_user = rail.RepliconServiceOperator(
            task_id='get_notification_preferences_for_user',
            endpoint="/services/NotificationScriptAdministrationService1.svc/GetUserNotificationPreferences",
            data=lambda: {
                "userUri": rail.result("get_user_details")["userDetails"]['uri']
            }
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

        get_user_assigned_role_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_assigned_role_from_replicon',
            endpoint='/services/ResourceService1.svc/GetProjectRoleAssignmentScheduleForUser',
            data=lambda: {
                "userUri": rail.result("get_user_details")["userDetails"]['uri']
            }
        )

        get_user_holiday_calendar = rail.RepliconServiceOperator(
            task_id='get_user_holiday_calendar',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidayCalendarAssignmentScheduleForUserAndDateRange",
            data=lambda dag_run: request_payload.get_user_holiday_cal_payload(dag_run, config.YMD_DATE_FORMAT),
            data_handler=response_filters.get_user_current_holiday_calendar
        )

        is_supervisor_present = rail.IfOperator(
            task_id='is_supervisor_present',
            test='{{ dag_run.conf.supervisor | is_truthy }}',
            yes_task='if_user_and_supervisor_same',
            no_task='get_update_user_payload'
        )

        if_user_and_supervisor_same = rail.IfOperator(
            task_id='if_user_and_supervisor_same',
            test='{{ dag_run.conf.email == dag_run.conf.supervisor }}',
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
                        "loginName": dag_run.conf.get("supervisor"),
                        "employeeId": null,
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
            no_task="if_supervisor_present_as_user_in_feed"
        )

        if_supervisor_present_as_user_in_feed = rail.IfOperator(
            task_id='if_supervisor_present_as_user_in_feed',
            test=lambda dag_run: dag_run.conf.get("supervisor") in custom_methods.get_all_user_login_names_from_feed(dag_run),
            yes_task='write_supervisor_pending_logs',
            no_task='get_update_user_payload'
        )

        write_supervisor_pending_logs = rail.WriteLogOperator(
            task_id="write_supervisor_pending_logs",
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor",
            severity="Pending",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "supervisor": dag_run.conf["supervisor"],
                "action": "Update",
                "user_uri": rail.result('get_user_details')["userDetails"]["uri"]
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
            data_handler=lambda response: rail.set_result(key="supervisor", val=response["supervisor"] if response else {})
        )

        get_update_user_payload = rail.PythonOperator(
            task_id="get_update_user_payload",
            python_callable=lambda dag_run: request_payload.get_update_user_req(dag_run, config),
        )

        if_update_user_payload_exists = rail.IfOperator(
            task_id="if_update_user_payload_exists",
            test=lambda: bool(rail.result("get_update_user_payload")),
            yes_task="update_user_details",
            no_task="if_orgrole_code_exists"
        )

        update_user_details = rail.RepliconServiceOperator(
            task_id="update_user_details",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: rail.result("get_update_user_payload"),
        )

        if_orgrole_code_exists = rail.IfOperator(
            task_id='if_orgrole_code_exists',
            test='{{ dag_run.conf.calculated_orgrole_code | is_truthy }}',
            yes_task='get_resource_pool_from_replicon',
            no_task='if_fte_or_schedule_or_location_or_level_changed'
        )

        get_resource_pool_from_replicon = rail.RepliconServiceOperator(
            task_id='get_resource_pool_from_replicon',
            endpoint='/services/ResourcePoolService1.svc/GetPageOfAvailableResourcePoolFilteredBySearchParameter',
            data=request_payload.get_resource_pools_payload,
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, "displayText", dag_run.conf.get("calculated_orgrole_code"))
        )

        is_resource_pool_present_in_replicon = rail.IfOperator(
            task_id='is_resource_pool_present_in_replicon',
            test='{{ result("get_resource_pool_from_replicon") | is_truthy }}',
            yes_task='get_user_assigned_resource_pools_from_replicon',
            no_task='if_fte_or_schedule_or_location_or_level_changed'
        )

        get_user_assigned_resource_pools_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_assigned_resource_pools_from_replicon',
            endpoint='/services/ResourcePoolService1.svc/GetPageOfResourcePoolsAssignedToUserFilteredBySearch',
            data=request_payload.get_user_assigned_resource_pools_payload,
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, "displayText", dag_run.conf.get("calculated_orgrole_code"))
        )

        if_resource_pool_already_assigned_to_user = rail.IfOperator(
            task_id='if_resource_pool_already_assigned_to_user',
            test='{{ result("get_user_assigned_resource_pools_from_replicon").uri | is_truthy }}',
            yes_task='if_fte_or_schedule_or_location_or_level_changed',
            no_task='assign_resource_pool_to_user'
        )

        assign_resource_pool_to_user = rail.RepliconServiceOperator(
            task_id='assign_resource_pool_to_user',
            endpoint='/services/ResourcePoolService1.svc/UpdateUserResourcePoolAssignment',
            data=lambda: request_payload.get_assign_resource_pool_payload(
                rail.result("update_user_details")["user"]["uri"])
        )

        # Time off policy assignment with comprehensive change detection
        if_fte_or_schedule_or_location_or_level_changed = rail.IfOperator(
            task_id="if_fte_or_schedule_or_location_or_level_changed",
            test=lambda dag_run: request_payload.check_fte_or_schedule_or_location_or_level_changes(dag_run).get('any_changes', False),
            yes_task="get_default_timeoff_policies",
            no_task="if_user_updated_with_exceptions"
        )

        # Get default policies for timeoff types after user update
        get_default_timeoff_policies = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_default_timeoff_policies",
            items=lambda dag_run: request_payload.get_updated_timeoff_types(dag_run).get("all_timeoff_types", []),
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda item: {"timeOffTypeUri": item},
            data_handler=lambda response, item: {
                "timeOffUri": item,
                "defaultPolicy": json.loads(json.dumps(
                    response, ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"'))
            }
        )

        build_timeoff_types_for_user_update = rail.PythonOperator(
            task_id="build_timeoff_types_for_user_update",
            python_callable=lambda dag_run: custom_methods.build_comprehensive_timeoff_assignments_for_update(dag_run, config)
        )

        if_timeoff_policies_calculated_for_update = rail.IfOperator(
            task_id="if_timeoff_policies_calculated_for_update",
            test=lambda: bool(rail.result("build_timeoff_types_for_user_update", {})),
            yes_task="if_location_or_level_changed",
            no_task="if_user_updated_with_exceptions"
        )

        # Only call assignment API when location/level changes
        if_location_or_level_changed = rail.IfOperator(
            task_id="if_location_or_level_changed",
            test=lambda dag_run: (request_payload.check_fte_or_schedule_or_location_or_level_changes(dag_run).get('is_location_changed') or
                request_payload.check_fte_or_schedule_or_location_or_level_changes(dag_run).get('is_level_changed')),
            yes_task="put_timeoff_assignment_for_user",
            no_task="assign_timeoff_policies_to_updated_user"
        )

        put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
            task_id='put_timeoff_assignment_for_user',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=request_payload.put_timeoff_assignment_payload
        )

        assign_timeoff_policies_to_updated_user = rail.RepliconServiceCallForEachItemOperator(
            task_id="assign_timeoff_policies_to_updated_user",
            items=lambda: rail.result("build_timeoff_types_for_user_update", []),
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda item: item
        )

        if_user_updated_with_exceptions = rail.IfOperator(
            task_id='if_user_updated_with_exceptions',
            test=lambda: bool(rail.result("update_user_details")[
                              "errors"][0]["notifications"]) if rail.result("update_user_details")["errors"] else False,
            yes_task='write_updated_user_with_exceptions_logs',
            no_task='write_updated_user_logs'
        )

        write_updated_user_with_exceptions_logs = rail.WriteLogOperator(
            task_id="write_updated_user_with_exceptions_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: "User partially updated with errors - " + " | ".join(
                request_payload.get_updated_logs(dag_run, config) + [details["displayText"]
                    for details in rail.result("update_user_details")["errors"][0]["notifications"]]
                        + request_payload.get_exception_logs(dag_run, config)),
            severity="Error",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": request_payload.get_action_type(dag_run, config),
                "status": "Error",
                "details": "User partially updated with errors - " + " | ".join(request_payload.get_updated_logs(dag_run, config) + 
                    [details["displayText"] for details in rail.result("update_user_details")["errors"][0]["notifications"]]
                        + request_payload.get_exception_logs(dag_run, config))
            }
        )

        write_updated_user_logs = rail.WriteLogOperator(
            task_id="write_updated_user_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: "User updated successfully" if not request_payload.get_exception_logs(dag_run, config) and
                    request_payload.get_updated_logs(dag_run, config) else
                        ("User partially updated - " + " | ".join(request_payload.get_updated_logs(dag_run, config)
                        + request_payload.get_exception_logs(dag_run, config))) if request_payload.get_updated_logs(dag_run, config)
                            else ("User not updated - " + " | ".join(request_payload.get_exception_logs(dag_run, config))
                                if request_payload.get_exception_logs(dag_run, config) else "User not updated"),
            severity=lambda dag_run: ("Success" if not request_payload.get_exception_logs(dag_run, config) and
                    request_payload.get_updated_logs(dag_run, config) else ("Exception" if request_payload.get_exception_logs(
                        dag_run, config) else "Skipped")),
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": request_payload.get_action_type(dag_run, config),
                "status": ("Success" if not request_payload.get_exception_logs(dag_run, config) and
                    request_payload.get_updated_logs(dag_run, config) else ("Exception" if request_payload.get_exception_logs(
                        dag_run, config) else "Skipped")),
                "details": "User updated successfully" if not request_payload.get_exception_logs(dag_run, config) and
                    request_payload.get_updated_logs(dag_run, config) else
                        ("User partially updated - " + " | ".join(request_payload.get_updated_logs(dag_run, config)
                        + request_payload.get_exception_logs(dag_run, config))) if request_payload.get_updated_logs(dag_run, config)
                            else ("User not updated - " + " | ".join(request_payload.get_exception_logs(dag_run, config))
                                if request_payload.get_exception_logs(dag_run, config) else "User not updated"),
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
                "employeeid": dag_run.conf["employee_id"],
                "action": request_payload.get_action_type(dag_run, config),
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_user_details >> if_user_exists_in_replicon
        if_user_exists_in_replicon >> rail.Label("No") >> log_user_not_present_in_replicon >> catch_and_log_errors
        if_user_exists_in_replicon >> rail.Label("Yes") >> get_notification_preferences_for_user >> get_current_group_membership \
            >> get_user_assigned_role_from_replicon >> get_user_holiday_calendar >> is_supervisor_present
        
        is_supervisor_present >> rail.Label("No") >> get_update_user_payload
        is_supervisor_present >> rail.Label("Yes") >> if_user_and_supervisor_same >> rail.Label("No") >> get_supervisor_details \
                    >> if_supervisor_exists >> rail.Label("Yes") >> if_supervisor_permission_exists
        if_user_and_supervisor_same >> rail.Label("Yes") >> get_update_user_payload >> if_update_user_payload_exists
        if_supervisor_exists >> rail.Label("No") >> if_supervisor_present_as_user_in_feed
        if_supervisor_present_as_user_in_feed >> rail.Label("No") >> get_update_user_payload
        if_supervisor_present_as_user_in_feed >> rail.Label("Yes") >> write_supervisor_pending_logs >> get_update_user_payload >> if_update_user_payload_exists
        if_supervisor_permission_exists >> rail.Label(
            "No") >> assign_supervisor_permission >> get_supervisor_assignment_details >> get_update_user_payload >> if_update_user_payload_exists
        if_supervisor_permission_exists >> rail.Label("Yes") >> get_supervisor_assignment_details >> get_update_user_payload >> if_update_user_payload_exists
        
        if_update_user_payload_exists >> rail.Label("Yes") >> update_user_details >> if_orgrole_code_exists
        if_update_user_payload_exists >> rail.Label("No") >> if_orgrole_code_exists

        if_orgrole_code_exists >> rail.Label("Yes") >> get_resource_pool_from_replicon >> is_resource_pool_present_in_replicon
        if_orgrole_code_exists >> rail.Label("No") >> if_fte_or_schedule_or_location_or_level_changed

        is_resource_pool_present_in_replicon >> rail.Label("Yes") >> get_user_assigned_resource_pools_from_replicon >> if_resource_pool_already_assigned_to_user

        if_resource_pool_already_assigned_to_user >> rail.Label("No") >> assign_resource_pool_to_user >> if_fte_or_schedule_or_location_or_level_changed
        if_resource_pool_already_assigned_to_user >> rail.Label("Yes") >> if_fte_or_schedule_or_location_or_level_changed

        is_resource_pool_present_in_replicon >> rail.Label("No") >> if_fte_or_schedule_or_location_or_level_changed
        
        # Timeoff processing after user update with comprehensive change detection
        if_fte_or_schedule_or_location_or_level_changed >> rail.Label("Yes") >> get_default_timeoff_policies >> build_timeoff_types_for_user_update >> if_timeoff_policies_calculated_for_update
        if_fte_or_schedule_or_location_or_level_changed >> rail.Label("No") >> if_user_updated_with_exceptions
        
        if_timeoff_policies_calculated_for_update >> rail.Label("Yes") >> if_location_or_level_changed
        if_location_or_level_changed >> rail.Label("Yes") >> put_timeoff_assignment_for_user >> assign_timeoff_policies_to_updated_user >> if_user_updated_with_exceptions
        if_location_or_level_changed >> rail.Label("No") >> assign_timeoff_policies_to_updated_user
        if_timeoff_policies_calculated_for_update >> rail.Label("No") >> if_user_updated_with_exceptions
        if_user_updated_with_exceptions >> rail.Label(
            "No") >> write_updated_user_logs >> finish_user_update
        if_user_updated_with_exceptions >> rail.Label(
            "Yes") >> write_updated_user_with_exceptions_logs >> finish_user_update
        
        finish_user_update >> catch_and_log_errors

        return dag

# Create child DAG for each instance
rail.for_each_instance(create_update_user_child_dag)