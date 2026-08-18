from datetime import timedelta
import json
import rail
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_todays_date_in_json
from dxctechnology.workday_user_import.user_import_portugal.utils import request_payload, custom_methods
from dxctechnology.workday_user_import.user_import_portugal.tasks.supervisor_assignment import assign_supervisor
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import should_trigger_delete_time_and_timeoff_for_disabled_user

null = None

def create_update_user_dag(config):
    
    with rail.create_airflow_dag(
        dag_id = config.portugal_update_user_dag_id,
        description = "add user",
        max_active_runs = 5,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_user_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_user_details",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id = "get_user_details",
            endpoint = "/services/ImportService1.svc/BulkGetUsers3",
            data = lambda dag_run: {
                "users": [
                    {
                        "uri": dag_run.conf['user_uri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else {}
        )

        get_effective_group_membership = rail.RepliconServiceOperator(
            task_id="get_effective_group_membership",
            endpoint="services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "dateRange": None
            },
            data_handler=custom_methods.get_effective_grp_membership_data_handler
        )

        get_user_timeoff_policy_summary = rail.RepliconServiceOperator(
            task_id = "get_user_timeoff_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri" : "{{ dag_run.conf.user_uri }}"
            }
        )

        get_user_assigned_policy = rail.RepliconServiceOperator(
            task_id = "get_user_assigned_policy",
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                "userUri" : "{{ dag_run.conf.user_uri }}"
            }
        )
        
        get_assigned_permission_for_user = rail.RepliconServiceOperator(
            task_id="get_assigned_permission_for_user",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        get_time_entry_approval_path = rail.RepliconServiceOperator(
            task_id = "get_time_entry_approval_path",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/GetApprovalPathForUser",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        is_user_disabled_for_non_go_live_country = rail.IfOperator(
            task_id = "is_user_disabled_for_non_go_live_country",
            test = lambda dag_run:custom_methods.is_user_disabled_for_non_go_live_country(dag_run, get_user_details.task_id),
            yes_task = "has_no_project_management_permission",
            no_task = "empty_is_user_disabled_and_replicon_field_false"
        )

        empty_is_user_disabled_and_replicon_field_false = rail.EmptyOperator(
            task_id = "empty_is_user_disabled_and_replicon_field_false"
        )
        
        has_no_project_management_permission = rail.IfOperator(
            task_id = "has_no_project_management_permission",
            test=custom_methods.user_has_no_project_management_permission_test,
            yes_task="get_direct_reports_for_user",
            no_task="empty_has_no_project_management_permission_no_task"
        )

        empty_has_no_project_management_permission_no_task = rail.EmptyOperator(
            task_id="empty_has_no_project_management_permission_no_task"
        )
        
        get_direct_reports_for_user = rail.RepliconServiceOperator(
            task_id = "get_direct_reports_for_user",
            endpoint = "/services/UserService1.svc/GetDirectReportsForUser",
            data = {
                "userUri": "{{dag_run.conf.user_uri}}",
                "asOfDate": None,
                "userStatusOptionUri": "urn:replicon:user-status-option:include-all-users"
            }
        )

        has_no_direct_reports_for_user = rail.IfOperator(
            task_id = "has_no_direct_reports_for_user",
            test="{{result('get_direct_reports_for_user') | is_falsy}}",
            yes_task="is_division_gsap",
            no_task="empty_has_no_direct_reports_for_user_no_task"
        )

        empty_has_no_direct_reports_for_user_no_task = rail.EmptyOperator(
            task_id = "empty_has_no_direct_reports_for_user_no_task"
        )

        is_division_gsap = rail.IfOperator(
            task_id = "is_division_gsap",
            test=custom_methods.is_division_gsap_test,
            yes_task="is_termination_date_present",
            no_task="disable_user_login_3"
        )
        is_termination_date_present = rail.IfOperator(
            task_id = "is_termination_date_present",
            test="{{dag_run.conf.file_data.term_date | is_truthy}}",
            yes_task="update_user_end_date",
            no_task="user_does_not_have_admin_and_payroll_permission"
        )

        update_user_end_date = rail.RepliconServiceOperator(
            task_id = "update_user_end_date",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": dag_run.conf['json_formatted_dates']['term_date'],
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )

        disable_user_login = rail.RepliconServiceOperator(
            task_id = "disable_user_login",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        def get_trigger_process_timeoff_policies_items():
            current_timeoff_policies = rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType']

            return list(filter(lambda to_policy:  to_policy['isTimeOffAllowedAgainstThisTimeOffType'] is True
                                and bool(to_policy['policySetSchedule'] and to_policy['policySetSchedule'][0]['effectiveDate'].get('day')), current_timeoff_policies))

        trigger_process_timeoff_policies = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_process_timeoff_policies",
            trigger_dag_id=config.workday_user_import_global_users_update_user_timeoff_process_child_dag_disable,
            items=get_trigger_process_timeoff_policies_items,
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf["master_file_name"],
                "user_uri": dag_run.conf["user_uri"],
                "timeoff_uri": item['timeOffType']["uri"],
                "policy_set": json.dumps(item["policySetSchedule"]).replace("[[{", "[{").replace("}]]", "}]"),
                "end_date": dag_run.conf['file_data']['term_date'],
                "end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "prevent_balance_overdraw_uri": dag_run.conf["prevent_balance_overdraw_uri"],
                "parent_location" : rail.result("get_effective_group_membership")['parent_location']
            }
        )

        wait_for_process_timeoff_policies = rail.WaitForDagRunsSensor(
            task_id = "wait_for_process_timeoff_policies",
            dag_runs="{{result('trigger_process_timeoff_policies')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
        )        

        log_disabled_user_22 =  rail.WriteLogOperator(
            task_id = "log_disabled_user_22",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Success",
            properties=lambda dag_run: custom_methods.get_disable_user_log_message(dag_run)
        )

        user_does_not_have_admin_and_payroll_permission = rail.IfOperator(
            task_id= "user_does_not_have_admin_and_payroll_permission",
            test=custom_methods.user_does_not_have_admin_and_payroll_permission_test,
            yes_task="disable_user_login_2",
            no_task="is_user_disabled_and_replicon_field_false"
        )
        
        disable_user_login_2 = rail.RepliconServiceOperator(
            task_id = "disable_user_login_2",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        can_update_user_end_date_3 = rail.IfOperator(
            task_id = "can_update_user_end_date_3",
            test=custom_methods.can_update_user_end_date_test,
            yes_task="update_user_end_date_4",
            no_task="trigger_process_timeoff_policies"
        )

        update_user_end_date_4 = rail.RepliconServiceOperator(
            task_id = "update_user_end_date_4",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": dag_run.conf['json_formatted_dates']['term_date'],
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )

        disable_user_login_3 = rail.RepliconServiceOperator(
            task_id = "disable_user_login_3",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        can_update_user_end_date = rail.IfOperator(
            task_id = "can_update_user_end_date",
            test=custom_methods.can_update_user_end_date_test,
            yes_task="update_user_end_date_2",
            no_task="trigger_process_timeoff_policies"
        )

        update_user_end_date_2 = rail.RepliconServiceOperator(
            task_id = "update_user_end_date_2",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": dag_run.conf['json_formatted_dates']['term_date'],
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )


        is_user_disabled_and_replicon_field_false = rail.IfOperator(
            task_id = "is_user_disabled_and_replicon_field_false",
            test= custom_methods.is_user_disabled_and_replicon_field_false_test,
            yes_task="dummy_is_user_disabled_and_replicon_field_false_yes_task",
            no_task="is_user_rehire"
        )

        dummy_is_user_disabled_and_replicon_field_false_yes_task= rail.EmptyOperator(
            task_id = "dummy_is_user_disabled_and_replicon_field_false_yes_task"
        )

        can_update_user_end_date_2 = rail.IfOperator(
            task_id = "can_update_user_end_date_2",
            test=custom_methods.can_update_user_end_date_test,
            yes_task= "update_user_end_date_3",
            no_task="log_disabled_user_46"
        )

        update_user_end_date_3 = rail.RepliconServiceOperator(
            task_id = "update_user_end_date_3",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": dag_run.conf['json_formatted_dates']['term_date'],
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )

        log_disabled_user_46 =  rail.WriteLogOperator(
            task_id = "log_disabled_user_46",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Skipped",
            properties=lambda dag_run: custom_methods.get_error_message_for_long_leave_or_user_disabled_with_replicon_field_false(dag_run)
        )

        is_user_rehire = rail.IfOperator(
            task_id = "is_user_rehire",
            test=custom_methods.is_user_rehire_test,
            yes_task="enable_login",
            no_task="can_update_user_start_date"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id = "enable_login",
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri" : "{{dag_run.conf.user_uri}}"
            }
        )
        
        update_user_start_date = rail.RepliconServiceOperator(
            task_id = "update_user_start_date",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": None,
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )

        can_update_user_start_date = rail.IfOperator(
            task_id = "can_update_user_start_date",
            test= custom_methods.can_update_user_start_date_test,
            yes_task="update_user_start_date2",
            no_task="should_disable_user"
        )
        
        update_user_start_date2 = rail.RepliconServiceOperator(
            task_id = "update_user_start_date2",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data= lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        should_disable_user = rail.IfOperator(
            task_id = "should_disable_user",
            test=custom_methods.should_disabled_user_test,
            yes_task="update_user_end_date_5",
            no_task="prepare_update_payload"
        )

        update_user_end_date_5 = rail.RepliconServiceOperator(
            task_id = "update_user_end_date_5",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": dag_run.conf['json_formatted_dates']['term_date'],
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )

        is_end_date_less_than_today = rail.IfOperator(
            task_id = "is_end_date_less_than_today",
            test=custom_methods.is_end_date_less_than_today_test,
            yes_task="disable_user_login_5",
            no_task="prepare_update_payload"
        )

        disable_user_login_5 = rail.RepliconServiceOperator(
            task_id = "disable_user_login_5",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        prepare_update_payload = rail.PythonOperator(
            task_id = "prepare_update_payload",
            python_callable=lambda dag_run: request_payload.get_update_user_payload(dag_run, config)
        )

        can_process_user = rail.IfOperator(
            task_id = "can_process_user",
            test=lambda: not bool(rail.result("prepare_update_payload", "ia_exception_msg")),
            yes_task="update_user",
            no_task="log_ia_exception"
        )

        log_ia_exception = rail.WriteLogOperator(
            task_id = "log_ia_exception",
            log = "{{dag_run.conf.user_log}}",
            message="User Update",
            severity="Skipped",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Exception",
                "Details": rail.result("prepare_update_payload", "ia_exception_msg")
            }
        )

        update_user = rail.RepliconServiceOperator(
            task_id = "update_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data = lambda: rail.result("prepare_update_payload")
        )

        can_update_notification_preference = rail.IfOperator(
            task_id = "can_update_notification_preference",
            test=lambda: rail.result('update_user', 'can_update_notification_pref') is True,
            yes_task="update_users_notification_preference",
            no_task="dummy_supervisor_assignment"
        )

        update_users_notification_preference = rail.RepliconServiceOperator(
            task_id = "update_users_notification_preference",
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data={
                "user": {
                    "uri": "{{dag_run.conf.user_uri}}"
                },
                "preferences": {
                    "notificationDeliveryPreferences": [
                    {
                        "objectTypeUri": "urn:replicon:object-type:project",
                        "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                    },
                    {
                        "objectTypeUri": "urn:replicon:object-type:user",
                        "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                    },
                    {
                        "objectTypeUri": "urn:replicon:object-type:timesheet",
                        "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                    },
                    {
                        "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                        "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                    },
                    {
                        "objectTypeUri": "urn:replicon:object-type:expense-sheet",
                        "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                    },
                    {
                        "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                        "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                    },
                    {
                        "objectTypeUri": "urn:replicon:object-type:time-off",
                        "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                    },
                    {
                        "objectTypeUri": "urn:replicon:object-type:holiday",
                        "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                    }
                    ],
                    "sharedDeliveryPreferenceOptionUris": [
                    "urn:replicon:user-shared-delivery-preference-option:always-deliver"
                    ]
                }
                }
        )

        trigger_timeoff_update_for_user = rail.TriggerDagRunOperator(
            task_id = "trigger_timeoff_update_for_user",
            trigger_dag_id=config.portugal_update_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
               **{
                   "feed_file_name": dag_run.conf["master_file_name"],
                    "user's_uri": dag_run.conf["user_uri"],
                    "user_log_name": dag_run.conf['user_log'],
                    "emp_id": dag_run.conf['file_data']["emp_id"],
                    "email_id": dag_run.conf['file_data']["email_id"],
                    "loginName": rail.result('update_user')['loginName'],
                    "start_date": get_todays_date_in_json(),
                    "Contineousservicedate": dag_run.conf['json_formatted_dates']['service_date'],
                    "timetype": dag_run.conf['file_data']['time_type'],
                    "gender": dag_run.conf['file_data']['gender'],
                    "rehire": custom_methods.is_user_rehire_test(dag_run, "string_yes_no"),
                    "workshift" :request_payload._get_work_shift_value(dag_run.conf["file_data"]['work_shift']),
                    "end_date": dag_run.conf['file_data']['term_date'],
                    "end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
                    "company_code": dag_run.conf['file_data']['company_code'],
                    "parent_company_code": dag_run.conf['file_data']['parent_company'],
                    "country": dag_run.conf['file_data']['country'],
                    "parent_location" : rail.result("get_effective_group_membership")['parent_location'],
                    "ia_updated": null,
                    "ia_end_date": dag_run.conf['json_formatted_dates']['ia_end_date'],
                    "ia_start_date": dag_run.conf['json_formatted_dates']['ia_start_date'],
                    "assignment_type": dag_run.conf['file_data']['assignment_type'],
                    "prt_vacation_bps_bpsot": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), 'name', '[PRT] Vacation BPS BPSOT', default={})
                },
                **dag_run.conf
            }
        )

        wait_for_user_timeoff_update_completion = rail.WaitForDagRunsSensor(
            task_id = "wait_for_user_timeoff_update_completion",
            dag_runs="{{result('trigger_timeoff_update_for_user')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
        )

        dummy_supervisor_assignment = rail.EmptyOperator(
            task_id = "dummy_supervisor_assignment"
        )

        start_supervisor_update, end_supervisor_update = assign_supervisor("update_supervisor", "update")

        def get_log_message():
            exception_msg = rail.result('update_user', 'exception_log')
            if exception_msg:
                return f"User updated partially - {rail.smartjoin_by_delim(exception_msg, ',')}"
            return "User updated successfully"

        log_user_completion = rail.WriteLogOperator(
            task_id = "log_user_completion",
            message = "User Add",
            log="{{dag_run.conf.user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Update",
                "Status": "Exception" if bool(rail.result('update_user', 'exception_log')) else "Update",
                "Details": get_log_message()
            }
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Update",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_user_details

        get_user_details >> get_effective_group_membership >> get_user_timeoff_policy_summary >> get_user_assigned_policy >> get_time_entry_approval_path >> get_assigned_permission_for_user
        get_assigned_permission_for_user >> is_user_disabled_for_non_go_live_country >> rail.Label("No") >> empty_is_user_disabled_and_replicon_field_false >> is_user_disabled_and_replicon_field_false

        is_user_disabled_for_non_go_live_country >> rail.Label("Yes") >> has_no_project_management_permission >> rail.Label("Yes") >> get_direct_reports_for_user \
            >> has_no_direct_reports_for_user >> rail.Label("Yes") >> is_division_gsap >> rail.Label("Yes") >> is_termination_date_present \
            >> rail.Label("Yes")>> update_user_end_date >> disable_user_login >> trigger_process_timeoff_policies\
            >> wait_for_process_timeoff_policies >> log_disabled_user_22 >> rail.Label("No") >> catch_and_log_error
        
        has_no_project_management_permission >> rail.Label("No") >> empty_has_no_project_management_permission_no_task >> is_user_disabled_and_replicon_field_false
        has_no_direct_reports_for_user >> rail.Label("No") >> empty_has_no_direct_reports_for_user_no_task >> is_user_disabled_and_replicon_field_false
        is_termination_date_present >> rail.Label("No") >> user_does_not_have_admin_and_payroll_permission >> rail.Label("Yes") >> disable_user_login_2 \
           >> can_update_user_end_date_3 >> rail.Label("No") >> trigger_process_timeoff_policies
        can_update_user_end_date_3 >> rail.Label("Yes") >> update_user_end_date_4 >> trigger_process_timeoff_policies

        is_division_gsap >> rail.Label("No") >> disable_user_login_3 >> can_update_user_end_date >> rail.Label("Yes") \
            >> update_user_end_date_2 >> trigger_process_timeoff_policies
        
        user_does_not_have_admin_and_payroll_permission >> is_user_disabled_and_replicon_field_false >> rail.Label("No") >> is_user_rehire
        is_user_disabled_and_replicon_field_false >> rail.Label("Yes") >> dummy_is_user_disabled_and_replicon_field_false_yes_task >> can_update_user_end_date_2 >> rail.Label("Yes")\
            >> update_user_end_date_3 >> log_disabled_user_46 >> rail.Label("No") >> catch_and_log_error
        can_update_user_end_date_2 >> rail.Label("No") >> log_disabled_user_46

        can_update_user_end_date >> rail.Label("No") >> trigger_process_timeoff_policies
        
        is_user_rehire >> rail.Label("Yes") >> enable_login >> update_user_start_date >> can_update_user_start_date
        is_user_rehire >> rail.Label("No") >> can_update_user_start_date

        can_update_user_start_date >> rail.Label("No") >> should_disable_user >> rail.Label("No") >> prepare_update_payload
        can_update_user_start_date >> rail.Label("Yes") >> update_user_start_date2 >> should_disable_user >> rail.Label("Yes") >> update_user_end_date_5\
            >> is_end_date_less_than_today >> rail.Label("Yes")\
            >> disable_user_login_5 >> trigger_process_timeoff_policies
        
        is_end_date_less_than_today >> rail.Label("No") >> prepare_update_payload >> can_process_user >> rail.Label("Yes") >> update_user >> can_update_notification_preference >> rail.Label(
            "Yes") >> update_users_notification_preference >> dummy_supervisor_assignment >> start_supervisor_update
        can_process_user >> rail.Label("No") >> log_ia_exception >> rail.Label("No") >> catch_and_log_error
        can_update_notification_preference >> rail.Label("No") >> dummy_supervisor_assignment >> start_supervisor_update

        end_supervisor_update >> trigger_timeoff_update_for_user >> wait_for_user_timeoff_update_completion >> log_user_completion >> rail.Label("No") >> catch_and_log_error

        return dag

rail.for_each_instance(create_update_user_dag)
