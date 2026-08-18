from neology.user_import.utils import request_payload, custom_methods, response_filters
from airflow.models import Variable
import rail
null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_child_dagid,
        description=f'Neology BambooHR to Polaris User Sync Update Child DAG {config.instance}',
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
            no_task="get_user_details_from_replicon"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_user_details_from_replicon",
            end_task="catch_and_log_errors"
        )

        get_user_details_from_replicon = rail.RepliconServiceOperator(
            task_id="get_user_details_from_replicon",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_user_details_from_replicon(dag_run.conf["user_details"]["employeenumber"]),
            data_handler=lambda response: response[0] if response else null
        )

        get_current_group_membership = rail.RepliconServiceOperator(
            task_id="get_current_group_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda dag_run: {
                    "userUri": rail.result("get_user_details_from_replicon")["userDetails"]['uri'],
                    "dateRange": {
                        "startDate": dag_run.conf["process_start_time"],
                        "endDate": dag_run.conf["process_start_time"]
                    }
            },
            data_handler=response_filters.get_current_effective_groups
        )

        get_user_assigned_role_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_assigned_role_from_replicon',
            endpoint='/services/ResourceService1.svc/BulkGetProjectRoleAssignmentScheduleForUsers',
            data= lambda dag_run: {
                "userUris": [rail.result("get_user_details_from_replicon")["userDetails"]['uri']],
                "dateRange": {
                    "startDate": dag_run.conf["process_start_time"],
                    "endDate": dag_run.conf["process_start_time"],
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_user_holiday_calendar = rail.RepliconServiceOperator(
            task_id='get_user_holiday_calendar',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidayCalendarAssignmentScheduleForUserAndDateRange",
            data=request_payload.get_user_holiday_cal_payload,
            data_handler=custom_methods.get_user_current_holiday_calendar
        )

        if_user_and_supervisor_same = rail.IfOperator(
            task_id='if_user_and_supervisor_same',
            test='{{ dag_run.conf.user_details.employeenumber == dag_run.conf.user_details.supervisorid }}',
            yes_task='get_update_payload',
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
            yes_task='write_supervisor_pending_logs',
            no_task='get_update_payload'
        )

        write_supervisor_pending_logs = rail.WriteLogOperator(
            task_id="write_supervisor_pending_logs",
            log='{{ dag_run.conf.supervisor_pending_log }}',
            message="Supervisor pending",
            severity="Pending",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["user_details"]["employeenumber"],
                "supervisor_id": dag_run.conf["user_details"]["supervisorid"],
                "action": "Update",
                "user_uri": rail.result("get_user_details_from_replicon")["userDetails"]["uri"],
            }
        )

        if_supervisor_permission_exists = rail.IfOperator(
            task_id="if_supervisor_permission_exists",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                rail.result("get_supervisor_details")["permissionSets"],
                    "displayText", config.supervisor_permission_set[0], "uri")),
            yes_task="get_supervisor_assignment_details",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: request_payload.get_assign_supervisor_permission_payload(config.supervisor_permission_set[0])
        )

        get_supervisor_assignment_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_assignment_details",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": rail.result("get_user_details_from_replicon")["userDetails"]["uri"],
                "asOfDate": dag_run.conf["process_start_time"]
            },
            data_handler=lambda response: response["supervisor"] if response else null
        )

        get_assigned_policy_sets_for_user = rail.RepliconServiceOperator(
            task_id="get_assigned_policy_sets_for_user",
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                "userUri": "{{ result('get_user_details_from_replicon').userDetails.uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,
                "policyUri", "urn:replicon:policy:time-punch", "policySet.uri")
        )

        get_update_payload = rail.PythonOperator(
            task_id='get_update_payload',
            python_callable=lambda dag_run: request_payload.get_update_user_req(
                dag_run, config.required_employee_fields, config.licenses)
        )

        if_skip_user_update = rail.IfOperator(
            task_id='if_skip_user_update',
            test=custom_methods.should_skip_update,
            yes_task='write_updated_user_logs',
            no_task='update_user_details'
        )

        update_user_details = rail.RepliconServiceOperator(
            task_id="update_user_details",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data='{{ result("get_update_payload") | to_json }}'
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
            message=lambda dag_run: request_payload.get_update_user_error_notifications(dag_run),
            severity="Error",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["user_details"]["employeenumber"],
                "action": "Update",
                "status": "Error",
                "details": request_payload.get_update_user_error_notifications(dag_run),
            }
        )

        write_updated_user_logs = rail.WriteLogOperator(
            task_id="write_updated_user_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: request_payload.get_updated_and_exception_log_message(dag_run),
            severity=lambda dag_run: request_payload.get_updated_and_exception_log_status(dag_run),
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["user_details"]["employeenumber"],
                "action": "Update",
                "status": request_payload.get_updated_and_exception_log_status(dag_run),
                "details": request_payload.get_updated_and_exception_log_message(dag_run)
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
                "action": "Update",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_user_details_from_replicon >> get_current_group_membership \
            >> get_user_assigned_role_from_replicon >> get_user_holiday_calendar >> if_user_and_supervisor_same >> rail.Label("No") >> get_supervisor_details \
                    >> if_supervisor_exists >> rail.Label("Yes") >> if_supervisor_permission_exists
        if_user_and_supervisor_same >> rail.Label("Yes") >> get_update_payload
        if_supervisor_exists >> rail.Label("No") >> if_supervisor_present_in_payload
        if_supervisor_present_in_payload >> rail.Label("Yes") >> write_supervisor_pending_logs >> get_update_payload
        if_supervisor_present_in_payload >> rail.Label("No") >> get_update_payload
        if_supervisor_permission_exists >> rail.Label(
            "No") >> assign_supervisor_permission >> get_supervisor_assignment_details
        if_supervisor_permission_exists >> rail.Label("Yes") >> get_supervisor_assignment_details \
            >> get_assigned_policy_sets_for_user >> get_update_payload >> if_skip_user_update
        if_skip_user_update >> rail.Label("No") >> update_user_details >> if_user_updated_with_exceptions
        if_skip_user_update >> rail.Label("Yes") >> write_updated_user_logs
        if_user_updated_with_exceptions >> rail.Label(
            "No") >> write_updated_user_logs >> catch_and_log_errors
        if_user_updated_with_exceptions >> rail.Label(
            "Yes") >> write_updated_user_with_exceptions_logs >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
