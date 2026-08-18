from datetime import timedelta
import json
import rail
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from dxctechnology.workday_user_import_v1.user_import.common_utils.response_filter import get_effective_grp_membership_data_handler
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_minus_specified_days_date_in_json
from dxctechnology.workday_user_import_v1.user_import_usa_les.utils import request_payload, custom_methods
from dxctechnology.workday_user_import_v1.user_import_usa_les.tasks.supervisor_assignment import assign_supervisor
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import should_trigger_delete_time_and_timeoff_for_disabled_user

null = None

def create_update_user_dag(config):
    
    with rail.create_airflow_dag(
        dag_id = config.usa_lse_update_user_dag_id,
        description = "add user",
        max_active_runs = 10,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_usa_les, default_var='true').lower() == 'true',
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
            data_handler=get_effective_grp_membership_data_handler
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

        is_user_disabled_for_non_go_live_country = rail.IfOperator(
            task_id = "is_user_disabled_for_non_go_live_country",
            test = lambda dag_run:custom_methods.is_user_disabled_for_non_go_live_country_test(dag_run, get_user_details.task_id),
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

            return list(filter(lambda to_policy:  (to_policy['isTimeOffAllowedAgainstThisTimeOffType'] is True
                                and (bool(to_policy['policySetSchedule'] and to_policy['policySetSchedule'][0]['effectiveDate'].get('day')))), current_timeoff_policies))

        trigger_process_timeoff_policies = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_process_timeoff_policies",
            trigger_dag_id=config.process_time_off_accrual,
            items=get_trigger_process_timeoff_policies_items,
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf["master_file_name"],
                "user_uri": dag_run.conf["user_uri"],
                "timeoff_type_uri": item['timeOffType']["uri"],
                "policy_set": json.dumps(item["policySetSchedule"]).replace("[[{", "[{").replace("}]]", "}]"),
                "end_date": dag_run.conf['file_data']['term_date'],
                "user_end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
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
            properties=custom_methods.get_disable_user_log_message
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

        is_user_disabled_and_replicon_field_false = rail.IfOperator(
            task_id = "is_user_disabled_and_replicon_field_false",
            test= custom_methods.is_user_disabled_and_replicon_field_false_test,
            yes_task="dummy_is_user_disabled_and_replicon_field_false_yes_task",
            no_task="empty_is_user_already_disabled_and_on_leave"
        )

        empty_is_user_already_disabled_and_on_leave = rail.EmptyOperator(
            task_id = "empty_is_user_already_disabled_and_on_leave"
        )

        is_user_already_disabled_and_on_leave = rail.IfOperator(
            task_id = "is_user_already_disabled_and_on_leave",
            test = custom_methods.get_is_user_already_disabled_and_on_leave_test,
            yes_task = "log_is_user_already_disabled_and_on_leave",
            no_task = "should_disable_user"
        )

        log_is_user_already_disabled_and_on_leave = rail.WriteLogOperator(
            task_id = "log_is_user_already_disabled_and_on_leave",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Skipped",
            properties=lambda dag_run:{
            "Jobid": "",
            "Userid": dag_run.conf['file_data']["emp_id"],
            "Email": dag_run.conf['file_data']["email_id"],
            "Action": 'Update',
            "Status": "Skipped",
            "Details": 'User already disabled in Replicon for "On Leave" is set to 1 in feed file'
        }
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
            properties=custom_methods.get_error_message_for_long_leave_or_user_disabled_with_replicon_field_false
        )

        is_user_rehire = rail.IfOperator(
            task_id = "is_user_rehire",
            test=custom_methods.is_user_rehire_test,
            yes_task="is_user_on_leave",
            no_task="empty_is_user_for_long_leave_disable"
        )   

        empty_is_user_for_long_leave_disable = rail.EmptyOperator(
            task_id = "empty_is_user_for_long_leave_disable"
        )


        is_user_on_leave = rail.IfOperator(
            task_id = "is_user_on_leave",
            test=custom_methods.is_user_on_leave_test,
            yes_task="empty_can_update_user_end_date_2_task",
            no_task="enable_login"
        )

        empty_can_update_user_end_date_2_task = rail.EmptyOperator(
            task_id = "empty_can_update_user_end_date_2_task"
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

        rehire_value = rail.PythonOperator(
            task_id = "rehire_value",
            python_callable=lambda dag_run: "Yes" if dag_run.conf['file_data']['parent_company'] == "C1" else "No"
        )
        
        # rehire variable update 
        is_user_for_long_leave_disable = rail.IfOperator(
            task_id = "is_user_for_long_leave_disable",
            test=custom_methods.is_user_for_long_leave_disable_test,
            yes_task="disable_user_login_4",
            no_task="can_update_user_start_date"
        )

        disable_user_login_4 = rail.RepliconServiceOperator(
            task_id = "disable_user_login_4",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        can_update_user_end_date_3 = rail.IfOperator(
            task_id = "can_update_user_end_date_3",
            test=custom_methods.can_update_user_end_date_test,
            yes_task="update_user_end_date_4",
            no_task="get_user_timeoff_policy_summary_2"
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

        get_user_timeoff_policy_summary_2 = rail.RepliconServiceOperator(
            task_id = "get_user_timeoff_policy_summary_2",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        can_update_user_start_date = rail.IfOperator(
            task_id = "can_update_user_start_date",
            test= custom_methods.can_update_user_start_date_test,
            yes_task="update_user_start_date2",
            no_task="process_user_update"
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
            no_task="is_user_rehire"
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
            no_task="process_user_update"
        )

        disable_user_login_5 = rail.RepliconServiceOperator(
            task_id = "disable_user_login_5",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        process_user_update = rail.EmptyOperator(
            task_id = "process_user_update"
        )

        def has_any_user_exception_callable(dag_run):
            user_current_ia_value = rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_details")['userDetails']['customFieldValues'], "customField.displayText", 'International Assignee', 'text', default="")
            if (dag_run.conf['file_data']['is_ia'] and (dag_run.conf['file_data']['is_ia'] != user_current_ia_value) and (dag_run.conf['file_data']['parent_company']== "COMPASS")):
                if dag_run.conf['file_data']['is_ia'] in ["1", 1] and not dag_run.conf['file_data']['ia_start_date']:
                    return "User processing skipped as IAStart date not available for IA=1"
                if dag_run.conf['file_data']['is_ia'] in [0, "0"] and not dag_run.conf['file_data']['ia_end_date']:
                    return "User processing skipped as IAEnd date not available for IA=0"
                if dag_run.conf['file_data']['is_ia'] in ["1", 1] and (request_payload.convert_json_date_to_date(
                            dag_run.conf['json_formatted_dates']['ia_start_date']) < get_todays_minus_specified_days_date_in_json(days_in_number=5, return_type="date")):
                    return "User processing skipped as IAStart date in past for IA=1"
                if dag_run.conf['file_data']['is_ia'] in ["0", 0] and (request_payload.convert_json_date_to_date(
                            dag_run.conf['json_formatted_dates']['ia_end_date']) < get_todays_minus_specified_days_date_in_json(days_in_number=5, return_type="date")):
                    return "User processing skipped as IAEnd date in past for IA=0"
            return False

        has_any_user_exception = rail.PythonOperator(
            task_id = "has_any_user_exception",
            python_callable=has_any_user_exception_callable
        )

        can_update_user = rail.IfOperator(
            task_id = "can_update_user",
            test = lambda : not bool(rail.result("has_any_user_exception")),
            no_task = "log_user_processing_exception",
            yes_task= "update_user"
        )

        log_user_processing_exception = rail.WriteLogOperator(
            task_id = "log_user_processing_exception",
            log = "{{dag_run.conf.user_log}}",
            message="User Update",
            severity="Exception",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Exception",
                "Details": rail.result("has_any_user_exception")
            }
        )

        update_user = rail.RepliconServiceOperator(
            task_id = "update_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data = lambda dag_run: custom_methods.get_update_user_payload(dag_run, config)
        )
        
        dummy_supervisor_assignment = rail.EmptyOperator(
            task_id = "dummy_supervisor_assignment"
        )

        start_supervisor_assignment, end_supervisor_assignment = assign_supervisor("update_supervisor", "update")

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

        process_update_user_timeoff_assignment = rail.TriggerDagRunOperator(
            task_id = "process_update_user_timeoff_assignment",
            trigger_dag_id=config.usa_lse_update_user_timeoff_assignment_dag_id,
            conf=lambda dag_run:{
                "file_name": dag_run.conf['master_file_name'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "user_uri": rail.result('update_user')['uri'],
                "loginName": rail.result('update_user')['loginName'],
                "company_code": dag_run.conf['file_data']['company_code'],
                "parent_company_code": dag_run.conf['file_data']['parent_company'],
                "country": dag_run.conf['file_data']['country'],
                "workshift": dag_run.conf['file_data']['work_shift'],
                "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                "json_formatted_dates": dag_run.conf['json_formatted_dates'],
                "rehire": "Yes" if custom_methods.is_user_rehire_test(dag_run) else "No",
                "psg": dag_run.conf['mapper_data']['psg'],
                "work_schedule": dag_run.conf['mapper_data']['schedule_hours'],
                "schedule_changed_date": dag_run.conf['json_formatted_dates']['work_shift_effective_date'],
                "fte": dag_run.conf['file_data']['fte'],
                "is_ia_updated": "Yes" if rail.result("update_user", "ia_updated") else "No",
                "ia_updated": "Yes" if rail.result("update_user", "ia_updated") else "No",
                "is_ia": dag_run.conf['file_data']['is_ia'],
                "ia_end_date": dag_run.conf['file_data']['ia_end_date'],
                "ia_start_date": dag_run.conf['file_data']['ia_start_date'],
                "assignment_type": dag_run.conf['file_data']['assignment_type'],
                "schedule_change": ((null if dag_run.conf['file_data']['fte'] == rail.find_first_by_attr_and_get_attr(rail.result("get_user_details")['userDetails']['customFieldValues'],
                                        "customField.displayText", "FTE", "text", "") else "Yes") if dag_run.conf['file_data']['fte'] else null),
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "personnal_sub_area":dag_run.conf['file_data']['sub_area_code'], #neeed to check mapping
                "employee_group":dag_run.conf['file_data']['emp_group_code'],
                "employee_sub_group":dag_run.conf['file_data']['emp_subgroup_code'],
                "state": dag_run.conf['file_data']['state'],
                "employeetype":"Exempt – Salaried" if dag_run.conf['groups']['employee_type']['is_exempt'] else "Non Exempt - Hourly",
                "paygroup": dag_run.conf['file_data']['pay_group'],
            },
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        wait_for_timeoff_assignment = rail.WaitForDagRunsSensor(
            task_id = "wait_for_timeoff_assignment",
            dag_runs="""{{ result('process_update_user_timeoff_assignment') }}""",
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        
        def get_log_message():
            exception_msg = rail.result('update_user', 'exception_log')
            if exception_msg:
                return f"User updated partially - {rail.smartjoin_by_delim(exception_msg, ',')}"
            return "User updated successfully"


        log_user_completion = rail.WriteLogOperator(
            task_id = "log_user_completion",
            message = "User Update",
            log="{{dag_run.conf.user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Update",
                "Status": "Exception" if bool(rail.result('update_user', 'exception_log')) else "Success",
                "Details": get_log_message()
            }
        )


        should_trigger_delete_time_and_timeoff = rail.IfOperator(
            task_id="should_trigger_delete_time_and_timeoff",
            test=should_trigger_delete_time_and_timeoff_for_disabled_user,
            yes_task="trigger_cleanup_for_disabled_user",
            no_task="catch_and_log_error"
        )

        trigger_cleanup_for_disabled_user = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_cleanup_for_disabled_user",
            trigger_dag_id=config.delete_future_entries_child_dag_id,
            items=[1],
            execution_timeout=timedelta(days=1),
            conf={
                'user_uri': "{{ dag_run.conf.user_uri }}",
                'end_date': "{{ dag_run.conf.file_data.term_date }}",
            }
        )

        wait_for_trigger_cleanup_for_disabled_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_trigger_cleanup_for_disabled_user",
            dag_runs="{{ result('trigger_cleanup_for_disabled_user') }}",
            execution_timeout=timedelta(days=1)
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
        get_user_details >> get_effective_group_membership >> get_user_assigned_policy >> get_user_timeoff_policy_summary >> get_assigned_permission_for_user
        get_assigned_permission_for_user >> is_user_disabled_for_non_go_live_country >> rail.Label("No") >> empty_is_user_disabled_and_replicon_field_false >> is_user_disabled_and_replicon_field_false

        is_user_disabled_for_non_go_live_country >> rail.Label("Yes") >> has_no_project_management_permission >> rail.Label("Yes") >> get_direct_reports_for_user \
            >> has_no_direct_reports_for_user >> rail.Label("Yes") >> is_division_gsap >> rail.Label("Yes") >> is_termination_date_present \
            >> rail.Label("Yes")>> update_user_end_date >> disable_user_login >> trigger_process_timeoff_policies\
            >> wait_for_process_timeoff_policies >> log_disabled_user_22 >> should_trigger_delete_time_and_timeoff

        has_no_project_management_permission >> rail.Label("No") >> empty_has_no_project_management_permission_no_task >> is_user_disabled_and_replicon_field_false
        has_no_direct_reports_for_user >> rail.Label("No") >> empty_has_no_direct_reports_for_user_no_task >> is_user_disabled_and_replicon_field_false
        is_termination_date_present >> rail.Label("No") >> user_does_not_have_admin_and_payroll_permission >> rail.Label("Yes") >> disable_user_login_2 \
            >> trigger_process_timeoff_policies
        
        is_division_gsap >> rail.Label("No") >> disable_user_login_3 >> can_update_user_end_date >> rail.Label("Yes") \
            >> update_user_end_date_2 >> trigger_process_timeoff_policies
        can_update_user_end_date >> rail.Label("No") >> trigger_process_timeoff_policies

        user_does_not_have_admin_and_payroll_permission >> rail.Label("No") >> is_user_disabled_and_replicon_field_false
        is_user_disabled_and_replicon_field_false >> rail.Label("Yes") >> dummy_is_user_disabled_and_replicon_field_false_yes_task >> can_update_user_end_date_2 >> rail.Label("Yes")\
            >> update_user_end_date_3 >> log_disabled_user_46 >> should_trigger_delete_time_and_timeoff
        can_update_user_end_date_2 >> rail.Label("No") >> log_disabled_user_46
        
        is_user_disabled_and_replicon_field_false >> rail.Label("No") >> empty_is_user_already_disabled_and_on_leave >> is_user_already_disabled_and_on_leave
        is_user_already_disabled_and_on_leave >> rail.Label("Yes") >> log_is_user_already_disabled_and_on_leave >> should_trigger_delete_time_and_timeoff
        is_user_already_disabled_and_on_leave >> rail.Label("No") >> should_disable_user >> is_user_rehire >> rail.Label("Yes") >> is_user_on_leave\
            >> rail.Label("Yes") >> empty_can_update_user_end_date_2_task >> can_update_user_end_date_2
        
        should_disable_user >> is_user_rehire
        is_user_rehire >> rail.Label("No") >> empty_is_user_for_long_leave_disable >> is_user_for_long_leave_disable >> rail.Label("No")
        is_user_on_leave >> rail.Label("No") >> enable_login >> update_user_start_date >> rehire_value >> is_user_for_long_leave_disable

        is_user_for_long_leave_disable >> rail.Label("No") >> can_update_user_start_date
        is_user_for_long_leave_disable >> rail.Label("Yes") >> disable_user_login_4 >> can_update_user_end_date_3 >> rail.Label("Yes") \
            >> update_user_end_date_4 >> get_user_timeoff_policy_summary_2 >> trigger_process_timeoff_policies
        can_update_user_end_date_3 >> rail.Label("No") >> get_user_timeoff_policy_summary_2

        can_update_user_start_date >> rail.Label("No") >> process_user_update
        # should_disable_user >> rail.Label("No") >> 
        process_user_update >> has_any_user_exception
        can_update_user_start_date >> rail.Label("Yes") >> update_user_start_date2 >> process_user_update
        should_disable_user >> rail.Label("Yes") >> update_user_end_date_5 >> is_end_date_less_than_today >> rail.Label("Yes")\
            >> disable_user_login_5 >> trigger_process_timeoff_policies
        
        is_end_date_less_than_today >> rail.Label("No") >> process_user_update

        has_any_user_exception >> can_update_user >> rail.Label("Yes") >> update_user >> can_update_notification_preference >> rail.Label(
            "Yes") >> update_users_notification_preference >> dummy_supervisor_assignment
        can_update_notification_preference >> rail.Label("No") >> dummy_supervisor_assignment
        can_update_user >> rail.Label("No") >> log_user_processing_exception >> should_trigger_delete_time_and_timeoff
        dummy_supervisor_assignment >> start_supervisor_assignment
        end_supervisor_assignment >> process_update_user_timeoff_assignment >> wait_for_timeoff_assignment >> log_user_completion >> should_trigger_delete_time_and_timeoff
        should_trigger_delete_time_and_timeoff >> rail.Label("Yes") >> trigger_cleanup_for_disabled_user >> wait_for_trigger_cleanup_for_disabled_user >> catch_and_log_error
        should_trigger_delete_time_and_timeoff >> rail.Label("No") >> catch_and_log_error
        
    return dag

rail.for_each_instance(create_update_user_dag)
