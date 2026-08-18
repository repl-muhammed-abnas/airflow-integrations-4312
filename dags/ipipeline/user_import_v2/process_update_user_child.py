from datetime import timedelta
import json
from ipipeline.user_import_v2.utils import request_payload, response_filters, custom_methods
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

        get_user_current_assigned_policies = rail.RepliconServiceOperator(
            task_id='get_user_current_assigned_policies',
            endpoint='/services/PolicySetService1.svc/GetAssignedPolicySetsForUser',
            data=lambda: {
                "userUri": rail.result("get_user_details")["userDetails"]['uri']
            },
            data_handler=lambda response: {
                'current_overtime_request_template_uri': rail.find_first_by_attr_and_get_attr(
                    response, "policyUri", "urn:replicon:policy:work-authorization", "policySet.uri") if response else null
            }
        )

        get_user_overtime_request_authorization_path_uri = rail.RepliconServiceOperator(
            task_id='get_user_overtime_request_authorization_path_uri',
            endpoint='/services/WorkAuthorizationApprovalService1.svc/GetApprovalPathForUser',
            data=lambda: {
                "userUri": rail.result("get_user_details")["userDetails"]['uri']
            },
            data_handler=lambda response: response.get(
                "uri", "") if response else ""
        )

        get_user_holiday_calendar = rail.RepliconServiceOperator(
            task_id='get_user_holiday_calendar',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidayCalendarAssignmentScheduleForUserAndDateRange",
            data=lambda dag_run: request_payload.get_user_holiday_cal_payload(
                dag_run, config.YMD_DATE_FORMAT),
            data_handler=response_filters.get_user_current_holiday_calendar
        )

        is_supervisor_present = rail.IfOperator(
            task_id='is_supervisor_present',
            test='{{ dag_run.conf.supervisor | is_truthy }}',
            yes_task='if_user_and_supervisor_same',
            no_task='get_timeoff_changes'
        )

        if_user_and_supervisor_same = rail.IfOperator(
            task_id='if_user_and_supervisor_same',
            test='{{ dag_run.conf.email == dag_run.conf.supervisor }}',
            yes_task='get_timeoff_changes',
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
            test=lambda dag_run: dag_run.conf.get(
                "supervisor") in custom_methods.get_all_user_login_names_from_feed(dag_run),
            yes_task='write_supervisor_pending_logs',
            no_task='get_timeoff_changes'
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
            data=lambda: request_payload.get_assign_supervisor_permission_payload(
                config.defaults_mapper_data["supervisor_permission"])
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

        get_timeoff_changes = rail.PythonOperator(
            task_id="get_timeoff_changes",
            python_callable=lambda dag_run: request_payload.get_updated_timeoff_types(
                dag_run, config)
        )

        get_update_user_payload = rail.PythonOperator(
            task_id="get_update_user_payload",
            python_callable=lambda dag_run: request_payload.get_update_user_req(
                dag_run, config),
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
            no_task='get_default_timeoff_policies'
        )

        get_resource_pool_from_replicon = rail.RepliconServiceOperator(
            task_id='get_resource_pool_from_replicon',
            endpoint='/services/ResourcePoolService1.svc/GetPageOfAvailableResourcePoolFilteredBySearchParameter',
            data=request_payload.get_resource_pools_payload,
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, "displayText", dag_run.conf.get("calculated_orgrole_code"))
        )

        is_resource_pool_present_in_replicon = rail.IfOperator(
            task_id='is_resource_pool_present_in_replicon',
            test='{{ result("get_resource_pool_from_replicon") | is_truthy }}',
            yes_task='get_user_assigned_resource_pools_from_replicon',
            no_task='get_default_timeoff_policies'
        )

        get_user_assigned_resource_pools_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_assigned_resource_pools_from_replicon',
            endpoint='/services/ResourcePoolService1.svc/GetPageOfResourcePoolsAssignedToUserFilteredBySearch',
            data=request_payload.get_user_assigned_resource_pools_payload,
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, "displayText", dag_run.conf.get("calculated_orgrole_code"))
        )

        if_resource_pool_already_assigned_to_user = rail.IfOperator(
            task_id='if_resource_pool_already_assigned_to_user',
            test=lambda: bool(rail.result("get_user_assigned_resource_pools_from_replicon").get('uri')) if rail.result(
                "get_user_assigned_resource_pools_from_replicon") else False,
            yes_task='get_default_timeoff_policies',
            no_task='assign_resource_pool_to_user'
        )

        assign_resource_pool_to_user = rail.RepliconServiceOperator(
            task_id='assign_resource_pool_to_user',
            endpoint='/services/ResourcePoolService1.svc/UpdateUserResourcePoolAssignment',
            data=lambda: request_payload.get_assign_resource_pool_payload(
                rail.result("update_user_details")["user"]["uri"])
        )

        # Get default policies for all timeoff types
        get_default_timeoff_policies = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_default_timeoff_policies",
            items=lambda dag_run: [timeoff_config.get("uri") for timeoff_config in dag_run.conf.get("calculated_time_off_types", {}).values() if timeoff_config.get(
                "reference_logic_type") in config.timeoffs_with_accrual_logic] if dag_run.conf.get("calculated_time_off_types") else [],
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda item: {"timeOffTypeUri": item},
            data_handler=lambda response, item: {
                "timeOffUri": item,
                "defaultPolicy": response
            }
        )

        if_new_timeoff_types_with_accrual_logic_to_assign = rail.IfOperator(
            task_id="if_new_timeoff_types_with_accrual_logic_to_assign",
            test=lambda: bool(rail.result("get_timeoff_changes")[
                              "new_timeoffs_with_accrual_logic_to_assign"]),
            yes_task="trigger_new_timeoff_with_logic_assignment",
            no_task="if_existing_timeoff_types_with_accrual_logic_to_update"
        )

        trigger_new_timeoff_with_logic_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_new_timeoff_with_logic_assignment",
            items=lambda: rail.result("get_timeoff_changes")[
                "new_timeoffs_with_accrual_logic_to_assign"],
            trigger_dag_id=config.timeoff_with_logic_assignment_dag_id,
            conf=lambda dag_run, item: {
                "user_uri": rail.result("get_user_details")["userDetails"]['uri'],
                "user_start_date": dag_run.conf.get("start_date"),
                "proration_effective_date": rail.parse_date(dag_run.conf.get('current_date'), config.YMD_DATE_FORMAT),
                "current_date": dag_run.conf.get("current_date"),
                "default_policyset_schedule_for_timeoff": rail.find_first_by_attr_and_get_attr(rail.result(
                    "get_default_timeoff_policies"), "timeOffUri", item['uri'], 'defaultPolicy'),
                "timeoff_uri": item['uri'],
                "timeoff_type_name": item['timeoff_type_name'],
                "timeoff_reference_logic_type": item['reference_logic_type'],
                "employee_id": dag_run.conf.get("employee_id"),
                "seniority_level": dag_run.conf.get("seniority_level"),
                "schedule_hours": dag_run.conf.get("scheduled_hours"),
                "fte": dag_run.conf.get("fte"),
                "uksick": dag_run.conf.get("uksick"),
                "previous_existing_schedule_for_user": "",
                "previous_seniority_level": "",
                "is_only_seniority_level_changed": "",
                "existing_timeoff_policyset_schedule_for_timeoff": "",
                "action": "Add"
            },
        )

        wait_for_trigger_new_timeoff_with_logic_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_new_timeoff_with_logic_assignment',
            dag_runs='{{ result("trigger_new_timeoff_with_logic_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_responses_from_new_timeoff_assignments_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_new_timeoff_assignments_child',
            dag_runs='{{ result("trigger_new_timeoff_with_logic_assignment") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        if_error_in_gather_reponse_from_new_timeoff_assignment_dag_runs = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_new_timeoff_assignment_dag_runs',
            test=lambda: bool(rail.result("gather_responses_from_new_timeoff_assignments_child")) and "Error" in json.dumps(rail.result(
                "gather_responses_from_new_timeoff_assignments_child")[0]),
            yes_task="fail_with_error_in_process_new_timeoffs_with_logic_assignment_dag",
            no_task="if_existing_timeoff_types_with_accrual_logic_to_update",
        )

        fail_with_error_in_process_new_timeoffs_with_logic_assignment_dag = rail.FailOperator(
            task_id='fail_with_error_in_process_new_timeoffs_with_logic_assignment_dag',
            message="Error in workflow for assigning new timeoff types with logic for update user"
        )

        if_existing_timeoff_types_with_accrual_logic_to_update = rail.IfOperator(
            task_id="if_existing_timeoff_types_with_accrual_logic_to_update",
            test=lambda: bool(rail.result("get_timeoff_changes")[
                              "timeoffs_to_update"]),
            yes_task="get_changed_values_for_timeoff_update",
            no_task="if_user_updated_with_exceptions"
        )

        get_changed_values_for_timeoff_update = rail.PythonOperator(
            task_id="get_changed_values_for_timeoff_update",
            python_callable=request_payload.check_fte_or_schedule_or_seniority_changes
        )

        # Time off policy update with comprehensive change detection
        check_fte_schedule_seniority_change = rail.IfOperator(
            task_id="check_fte_schedule_seniority_change",
            test=lambda: rail.result('get_changed_values_for_timeoff_update').get(
                'any_changes', False),
            yes_task="trigger_update_timeoff_with_logic_assignment",
            no_task="if_user_updated_with_exceptions"
        )

        trigger_update_timeoff_with_logic_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_update_timeoff_with_logic_assignment",
            items=lambda: rail.result(
                "get_timeoff_changes")["timeoffs_to_update"],
            trigger_dag_id=config.timeoff_with_logic_assignment_dag_id,
            conf=lambda dag_run, item: request_payload.trigger_timeoff_with_logic_assignment_config(
                dag_run, item, config)
        )

        wait_for_trigger_update_timeoff_with_logic_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_update_timeoff_with_logic_assignment',
            dag_runs='{{ result("trigger_update_timeoff_with_logic_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_responses_from_existing_timeoff_update_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_existing_timeoff_update_child',
            dag_runs='{{ result("trigger_update_timeoff_with_logic_assignment") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        if_error_in_gather_responses_from_existing_timeoff_update_child = rail.IfOperator(
            task_id='if_error_in_gather_responses_from_existing_timeoff_update_child',
            test=lambda: bool(rail.result("gather_responses_from_existing_timeoff_update_child")) and "Error" in json.dumps(rail.result(
                "gather_responses_from_existing_timeoff_update_child")[0]),
            yes_task="fail_with_error_in_process_existing_timeoffs_with_logic_assignment_dag",
            no_task="if_user_updated_with_exceptions",
        )

        fail_with_error_in_process_existing_timeoffs_with_logic_assignment_dag = rail.FailOperator(
            task_id='fail_with_error_in_process_existing_timeoffs_with_logic_assignment_dag',
            message="Error in workflow for updating existing timeoff types with logic for update user"
        )

        if_user_updated_with_exceptions = rail.IfOperator(
            task_id='if_user_updated_with_exceptions',
            test=lambda: (bool(rail.result("update_user_details")[
                              "errors"][0]["notifications"]) if rail.result("update_user_details")["errors"] else False) if rail.result(
                                  "update_user_details") else False,
            yes_task='write_updated_user_with_exceptions_logs',
            no_task='write_updated_user_logs'
        )

        write_updated_user_with_exceptions_logs = rail.WriteLogOperator(
            task_id="write_updated_user_with_exceptions_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: ("User partially updated with errors - " + " | ".join(request_payload.get_updated_logs(
                dag_run, config) + [details["displayText"] for details in rail.result(
                    "update_user_details")["errors"][0]["notifications"]] + request_payload.get_exception_logs(
                        dag_run, config))),
            severity="Error",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": request_payload.get_action_type(dag_run, config),
                "status": "Error",
                "details": ("User partially updated with errors - " + " | ".join(request_payload.get_updated_logs(
                    dag_run, config) + [details["displayText"] for details in rail.result(
                        "update_user_details")["errors"][0]["notifications"]] + request_payload.get_exception_logs(
                            dag_run, config)))
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
        can_run_batch_task >> rail.Label(
            "No") >> get_user_details >> if_user_exists_in_replicon
        if_user_exists_in_replicon >> rail.Label(
            "No") >> log_user_not_present_in_replicon >> catch_and_log_errors
        if_user_exists_in_replicon >> rail.Label("Yes") >> get_notification_preferences_for_user >> get_current_group_membership \
            >> get_user_assigned_role_from_replicon >> get_user_current_assigned_policies \
            >> get_user_overtime_request_authorization_path_uri >> get_user_holiday_calendar >> is_supervisor_present

        is_supervisor_present >> rail.Label("No") >> get_timeoff_changes
        is_supervisor_present >> rail.Label("Yes") >> if_user_and_supervisor_same >> rail.Label("No") >> get_supervisor_details \
            >> if_supervisor_exists >> rail.Label("Yes") >> if_supervisor_permission_exists
        if_user_and_supervisor_same >> rail.Label("Yes") >> get_timeoff_changes
        if_supervisor_exists >> rail.Label(
            "No") >> if_supervisor_present_as_user_in_feed
        if_supervisor_present_as_user_in_feed >> rail.Label(
            "No") >> get_timeoff_changes
        if_supervisor_present_as_user_in_feed >> rail.Label(
            "Yes") >> write_supervisor_pending_logs >> get_timeoff_changes
        if_supervisor_permission_exists >> rail.Label(
            "No") >> assign_supervisor_permission >> get_supervisor_assignment_details >> get_timeoff_changes
        if_supervisor_permission_exists >> rail.Label(
            "Yes") >> get_supervisor_assignment_details >> get_timeoff_changes

        get_timeoff_changes >> get_update_user_payload >> if_update_user_payload_exists

        if_update_user_payload_exists >> rail.Label(
            "Yes") >> update_user_details >> if_orgrole_code_exists
        if_update_user_payload_exists >> rail.Label(
            "No") >> if_orgrole_code_exists

        if_orgrole_code_exists >> rail.Label(
            "Yes") >> get_resource_pool_from_replicon >> is_resource_pool_present_in_replicon
        if_orgrole_code_exists >> rail.Label(
            "No") >> get_default_timeoff_policies

        is_resource_pool_present_in_replicon >> rail.Label(
            "Yes") >> get_user_assigned_resource_pools_from_replicon >> if_resource_pool_already_assigned_to_user

        if_resource_pool_already_assigned_to_user >> rail.Label(
            "No") >> assign_resource_pool_to_user >> get_default_timeoff_policies
        if_resource_pool_already_assigned_to_user >> rail.Label(
            "Yes") >> get_default_timeoff_policies

        is_resource_pool_present_in_replicon >> rail.Label(
            "No") >> get_default_timeoff_policies

        # Timeoff processing after user update with comprehensive change detection
        get_default_timeoff_policies >> if_new_timeoff_types_with_accrual_logic_to_assign

        if_new_timeoff_types_with_accrual_logic_to_assign >> rail.Label(
            "No") >> if_existing_timeoff_types_with_accrual_logic_to_update
        if_new_timeoff_types_with_accrual_logic_to_assign >> rail.Label(
            "Yes") >> trigger_new_timeoff_with_logic_assignment >> wait_for_trigger_new_timeoff_with_logic_assignment \
            >> gather_responses_from_new_timeoff_assignments_child >> if_error_in_gather_reponse_from_new_timeoff_assignment_dag_runs \

        if_error_in_gather_reponse_from_new_timeoff_assignment_dag_runs >> rail.Label(
            "No") >> if_existing_timeoff_types_with_accrual_logic_to_update

        if_error_in_gather_reponse_from_new_timeoff_assignment_dag_runs >> rail.Label(
            "Yes") >> fail_with_error_in_process_new_timeoffs_with_logic_assignment_dag >> if_existing_timeoff_types_with_accrual_logic_to_update

        if_existing_timeoff_types_with_accrual_logic_to_update >> rail.Label(
            "No") >> if_user_updated_with_exceptions
        if_existing_timeoff_types_with_accrual_logic_to_update >> rail.Label(
            "Yes") >> get_changed_values_for_timeoff_update >> check_fte_schedule_seniority_change

        check_fte_schedule_seniority_change >> rail.Label(
            "No") >> if_user_updated_with_exceptions
        check_fte_schedule_seniority_change >> rail.Label(
            "Yes") >> trigger_update_timeoff_with_logic_assignment >> wait_for_trigger_update_timeoff_with_logic_assignment \
            >> gather_responses_from_existing_timeoff_update_child >> if_error_in_gather_responses_from_existing_timeoff_update_child

        if_error_in_gather_responses_from_existing_timeoff_update_child >> rail.Label(
            "No") >> if_user_updated_with_exceptions
        if_error_in_gather_responses_from_existing_timeoff_update_child >> rail.Label(
            "Yes") >> fail_with_error_in_process_existing_timeoffs_with_logic_assignment_dag >> if_user_updated_with_exceptions

        if_user_updated_with_exceptions >> rail.Label(
            "No") >> write_updated_user_logs >> finish_user_update
        if_user_updated_with_exceptions >> rail.Label(
            "Yes") >> write_updated_user_with_exceptions_logs >> finish_user_update

        finish_user_update >> catch_and_log_errors

        return dag


# Create child DAG for each instance
rail.for_each_instance(create_update_user_child_dag)
