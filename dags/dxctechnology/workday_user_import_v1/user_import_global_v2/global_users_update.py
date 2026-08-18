
from datetime import timedelta
import json
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_global_v2.utils import custom_methods
from dxctechnology.workday_user_import_v1.user_import_global_v2.utils.request_payload import prepare_update_user_payload_callable, NONE_DEFAULT_VALUE, _is_international_assignee
from dxctechnology.workday_user_import_v1.user_import.common_utils.response_filter import get_effective_grp_membership_data_handler
from dxctechnology.workday_user_import_v1.user_import_global_v2.task.supervisor_assignment_task import assign_supervisor
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import should_trigger_delete_time_and_timeoff_for_disabled_user
from dxctechnology.workday_user_import_v1.user_import.tasks.update_timesheet_template import update_timesheet_template_task

null = None

# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_global_v2_users_update_user_child_dag,
        description="dxctechnology workday user sync Master",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.global_update_user_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")
        
        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_global, default_var='true').lower() == 'true',
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
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{dag_run.conf.user_uri}}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else {}
        )

        get_user_assigned_policy = rail.RepliconServiceOperator(
                task_id = "get_user_assigned_policy",
                endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
                data={
                    "userUri" : "{{ dag_run.conf.user_uri }}"
                }
            )

        get_effective_group_membership = rail.RepliconServiceOperator(
            task_id="get_effective_group_membership",
            endpoint="services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{dag_run.conf.user_uri}}",
                "dateRange": None
            },
            data_handler=get_effective_grp_membership_data_handler
        )
        
        get_user_timeoff_policy_summary = rail.RepliconServiceOperator(
            task_id = "get_user_timeoff_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri" : "{{dag_run.conf.user_uri}}"
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
            trigger_dag_id=config.workday_user_import_global_v2_users_update_user_timeoff_process_child_dag_disable,
            items=get_trigger_process_timeoff_policies_items,
            conf=lambda item, dag_run: {
                "user_log": dag_run.conf['user_log'],
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
            properties=custom_methods.get_error_message_for_long_leave_or_user_disabled_with_replicon_field_false
        )

        is_user_rehire = rail.IfOperator(
            task_id = "is_user_rehire",
            test=custom_methods.is_user_rehire_test,
            yes_task="is_user_on_leave",
            no_task="is_user_for_long_leave_disable"
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
            no_task="is_country_canada_and_parent_company_code_c1"
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
                
        is_country_canada_and_parent_company_code_c1 = rail.IfOperator(
            task_id = "is_country_canada_and_parent_company_code_c1",
            test= lambda dag_run: custom_methods.compare_country_and_parent_company_code(dag_run, "Canada", "C1"),
            yes_task= "get_user_timeoff_policy_summary_2",
            no_task="log_disabled_user_22"
        )

        # May not needed this part
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
            no_task="process_user_update"
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

        is_country_canada_and_parent_company_code_c1_1 = rail.IfOperator(
            task_id = "is_country_canada_and_parent_company_code_c1_1",
            test= lambda dag_run: custom_methods.compare_country_and_parent_company_code(dag_run, "Canada", "C1"),
            yes_task= "trigger_process_timeoff_policies",
            no_task="log_disabled_user_22"
        )

        process_user_update = rail.EmptyOperator(
            task_id = "process_user_update"
        )

        get_time_entry_approval_path = rail.RepliconServiceOperator(
            task_id = "get_time_entry_approval_path",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/GetApprovalPathForUser",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        prepare_update_user_payload = rail.PythonOperator(
            task_id = "prepare_update_user_payload",
            python_callable=lambda dag_run: prepare_update_user_payload_callable(dag_run, config)
        )

        can_process_update_user = rail.IfOperator(
            task_id = "can_process_update_user",
            test=lambda: not bool(rail.result("prepare_update_user_payload", "ia_exception_msg")),
            yes_task = "update_user",
            no_task = "log_ia_exception"
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
                "Details": rail.result("prepare_update_user_payload", "ia_exception_msg")
            }
        )


        update_user = rail.RepliconServiceOperator(
            task_id = "update_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda : rail.result("prepare_update_user_payload")
        )

        start_timesheet_template_assignment, end_timesheet_template_assignment = update_timesheet_template_task(
            "timesheet_template_assignment",
            config,
            get_user_details.task_id)

        can_update_notification_preference = rail.IfOperator(
            task_id = "can_update_notification_preference",
            test=lambda: rail.result('prepare_update_user_payload', 'can_update_notification_pref') is True,
            yes_task="update_users_notification_preference",
            no_task="is_profile_enabled"
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

        is_profile_enabled = rail.IfOperator(
            task_id = "is_profile_enabled",
            test=custom_methods.is_profile_enabled,
            yes_task="can_update_holiday_calendar",
            no_task="get_assigned_schedule_policy_empty"
        )

        def can_update_holiday_calendar_test(dag_run):
            holiday_calendar = rail.result('get_user_details')['holidayCalendar'].get('displayText', '') if rail.result('get_user_details')['holidayCalendar'] else ''
            if dag_run.conf['mapper_data']['holiday_calendar']:
                if dag_run.conf['mapper_data']['holiday_calendar'] != holiday_calendar:
                    if dag_run.conf['mapper_data']['holiday_calendar_uri']:
                        return True
            # If no holiday calendar in mapper but user has one, assign NONE
            elif holiday_calendar != NONE_DEFAULT_VALUE and _is_international_assignee(dag_run.conf['file_data']['is_ia']):
                rail.set_result(key='use_none_holiday_calendar', val=True)
                return True
            return False

        can_update_holiday_calendar = rail.IfOperator(
            task_id = "can_update_holiday_calendar",
            test=can_update_holiday_calendar_test,
            yes_task="update_user_holiday_calendar",
            no_task="can_update_timeoff_template"
        )

        def get_holiday_calendar_update_payload(dag_run):
            use_none = rail.result('can_update_holiday_calendar', 'use_none_holiday_calendar')
            if use_none:
                return {
                    "userUri": dag_run.conf['user_uri'],
                    "holidayCalendar": {
                        "uri": null,
                        "name": NONE_DEFAULT_VALUE
                    }
                }
            return {
                "userUri": dag_run.conf['user_uri'],
                "holidayCalendarUri": dag_run.conf['mapper_data']['holiday_calendar_uri']
            }

        update_user_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_user_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data=get_holiday_calendar_update_payload
        )

        def can_update_timeoff_template_test(dag_run):
            timeoff_template = rail.result('get_user_details')['timeOffTemplate'].get('name', '') if rail.result('get_user_details')['timeOffTemplate'] else ''
            if dag_run.conf['mapper_data']['timeoff_template']:
                if dag_run.conf['mapper_data']['timeoff_template'] != timeoff_template:
                    if dag_run.conf['policy_sets']['timeoff_template'].get('uri'):
                        return True
                    rail.set_result(key='exception_msg', val=f'''Timeoff template "{dag_run.conf['mapper_data']['timeoff_template']}" not available in Replicon''')
            else:
                if rail.result('prepare_update_user_payload', 'ia_updated') or dag_run.conf['file_data']['is_ia'] in [1,'1']:
                    if not timeoff_template:# No timeoff template assigned and none in mapper, so no update needed
                        return False
                    if timeoff_template:
                        rail.set_result(key='exception_msg', val=f'''Timeoff template not available in mapper for IA == 1, removing existing template "{timeoff_template}"''')
                        rail.set_result(key="remove_timeoff_template", val="true")
                        return True
            return False

        can_update_timeoff_template = rail.IfOperator(
            task_id= "can_update_timeoff_template",
            test=can_update_timeoff_template_test,
            yes_task="can_update_timeoff_template_empty",
            no_task="log_exception" # this is python operator which will log the exception of step 254 in workato recipe
        )

        can_update_timeoff_template_empty = rail.EmptyOperator(
            task_id = "can_update_timeoff_template_empty"
        )

        check_remove_timeoff_template = rail.IfOperator(
            task_id="check_remove_timeoff_template",
            test=lambda: rail.result('can_update_timeoff_template', 'remove_timeoff_template') == "true",
            yes_task="remove_timeoff_template",
            no_task="update_timeoff_template"
        )

        remove_timeoff_template = rail.RepliconServiceOperator(
            task_id = "remove_timeoff_template",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data = lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['user_uri']
                },
                "modifications": {
                    "policySetsToApply": {
                        "policySetUrisToAssign": [],
                        "policyUrisToRemovePolicySet": [],
                        "policySetUrisToRemove": [
                                rail.result('get_user_details')['timeOffTemplate']['uri']
                            ]
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        update_timeoff_template = rail.RepliconServiceOperator(
            task_id = "update_timeoff_template",
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "policySetUri": "{{ dag_run.conf.policy_sets.timeoff_template.uri}}"
            }
        )

        def log_exception_callable():
            log = []
            if rail.result('can_update_holiday_calendar', 'exception_msg'):
                log.append(rail.result('can_update_holiday_calendar', 'exception_msg'))
            if rail.result('can_update_timeoff_template', 'exception_msg'):
                log.append(rail.result('can_update_timeoff_template', 'exception_msg'))
            return log

        log_exception = rail.PythonOperator(
            task_id = "log_exception",
            python_callable=log_exception_callable
        )

        def get_assigned_schedule_policy_callable(dag_run):
            assigned_policies = rail.result('get_user_assigned_policy')
            for policy in assigned_policies:
                if policy['policyUri'] == "urn:replicon:policy:shift-schedule":
                    return policy
            return None

        get_assigned_schedule_policy_empty = rail.EmptyOperator(
            task_id = "get_assigned_schedule_policy_empty"
        )

        get_assigned_schedule_policy = rail.PythonOperator(
            task_id = "get_assigned_schedule_policy",
            python_callable=get_assigned_schedule_policy_callable
        )

        def check_remove_schedule_policy_test(dag_run):
            if rail.result('prepare_update_user_payload', 'ia_updated') and dag_run.conf['file_data']['is_ia'] in [1,'1'] and rail.result('get_assigned_schedule_policy'):
                return True
            return False

        check_remove_schedule_policy = rail.IfOperator(
            task_id="check_remove_schedule_policy",
            test=check_remove_schedule_policy_test,
            yes_task="remove_schedule_policy",
            no_task="trigger_timeoff_update_for_user"
        )

        remove_schedule_policy = rail.RepliconServiceOperator(
            task_id = "remove_schedule_policy",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data = lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['user_uri']
                },
                "modifications": {
                    "policySetsToApply": {
                        "policySetUrisToAssign": [],
                        "policyUrisToRemovePolicySet": [],
                        "policySetUrisToRemove": [
                                rail.result('get_assigned_schedule_policy')['policySet']['uri']
                            ]
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        # Timeoff policy assignments 
        trigger_timeoff_update_for_user = rail.TriggerDagRunOperator(
            task_id = "trigger_timeoff_update_for_user",
            trigger_dag_id=config.workday_user_import_global_v2_users_update_user_timeoff_process_child_dag,
            conf=lambda dag_run: {
                "file_name": dag_run.conf["master_file_name"],
                "user_uri": dag_run.conf["user_uri"],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']["emp_id"],
                "email_id": dag_run.conf['file_data']["email_id"],
                "loginName": rail.result('update_user')['loginName'],
                "start_date": {},
                "hire_date": dag_run.conf['file_data']['hire_date'],
                "end_date": dag_run.conf['file_data']['term_date'],
                "end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
                "company_code": dag_run.conf['file_data']['company_code'],
                "parent_company_code": dag_run.conf['file_data']['parent_company'],
                "country": dag_run.conf['file_data']['country'],
                "timeoffs": dag_run.conf['mapper_data']['timeoffs'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "prevent_balance_overdraw_uri": dag_run.conf["prevent_balance_overdraw_uri"],
                "parent_location" : rail.result("get_effective_group_membership")['parent_location'],
                "is_ia": dag_run.conf['file_data']['is_ia'],
                "ia_updated": rail.result("prepare_update_user_payload", "ia_updated"),
                "ia_end_date": dag_run.conf['json_formatted_dates']['ia_end_date'],
                "ia_start_date": dag_run.conf['json_formatted_dates']['ia_start_date'],
                "assignment_type": dag_run.conf['file_data']['assignment_type']
            }
        )

        wait_for_user_timeoff_update_completion = rail.WaitForDagRunsSensor(
            task_id = "wait_for_user_timeoff_update_completion",
            dag_runs="{{result('trigger_timeoff_update_for_user')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
        )

        supervisor_start, supervisor_end = assign_supervisor("update_user_supervisor_assignment", "update")

        def get_user_update_log_message(dag_run):
            log_exception = rail.result('log_exception') or []
            log_timesheet_template_exception = [rail.result('log_timesheet_template_exception')] or []
            exception_message = rail.result("prepare_update_user_payload", "exception_log") + log_exception + log_timesheet_template_exception
            return{
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Success" if not exception_message else "Exception",
                "Details": "Updated successfully" if not exception_message else f"Updated partially - {rail.smartjoin_by_delim(exception_message, ',')}"
            }

        log_user_success =  rail.WriteLogOperator(
            task_id = "log_user_success",
            log = "{{dag_run.conf.user_log}}",
            message="User Update",
            severity="Success",
            properties=get_user_update_log_message
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

        get_user_details >> get_user_assigned_policy >> get_effective_group_membership >> get_user_timeoff_policy_summary >> get_assigned_permission_for_user
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
        
        is_user_disabled_and_replicon_field_false >> rail.Label("No") >> is_user_rehire >> rail.Label("Yes") >> is_user_on_leave\
            >> rail.Label("Yes") >> empty_can_update_user_end_date_2_task >> can_update_user_end_date_2
        
        is_user_rehire >> rail.Label("No") >> is_user_for_long_leave_disable >> rail.Label("No") >> can_update_user_start_date
        is_user_on_leave >> rail.Label("No") >> enable_login >> update_user_start_date >> is_user_for_long_leave_disable

        is_user_for_long_leave_disable >> rail.Label("Yes") >> disable_user_login_4 >> can_update_user_end_date_3 >> rail.Label("Yes") \
            >> update_user_end_date_4 >> is_country_canada_and_parent_company_code_c1 >> rail.Label("Yes") >> get_user_timeoff_policy_summary_2 >> trigger_process_timeoff_policies
        is_country_canada_and_parent_company_code_c1 >> rail.Label("No") >> log_disabled_user_22
        can_update_user_end_date_3 >> rail.Label("No") >> is_country_canada_and_parent_company_code_c1

        can_update_user_start_date >> rail.Label("No") >> should_disable_user >> rail.Label("No") >> process_user_update
        can_update_user_start_date >> rail.Label("Yes") >> update_user_start_date2 >> should_disable_user >> rail.Label("Yes") >> update_user_end_date_5 >> is_end_date_less_than_today >> rail.Label("Yes")\
            >> disable_user_login_5 >> is_country_canada_and_parent_company_code_c1_1 >> rail.Label("Yes") >> trigger_process_timeoff_policies
        is_country_canada_and_parent_company_code_c1_1 >> rail.Label("No") >> log_disabled_user_22
        
        is_end_date_less_than_today >> rail.Label("No") >> process_user_update >> get_time_entry_approval_path >> prepare_update_user_payload \
            >> can_process_update_user >> rail.Label("Yes") >> update_user
        
        can_process_update_user >> rail.Label("No") >> log_ia_exception >> should_trigger_delete_time_and_timeoff

        update_user >> start_timesheet_template_assignment 
        end_timesheet_template_assignment >> can_update_notification_preference >> rail.Label("Yes") >> update_users_notification_preference >> is_profile_enabled
        can_update_notification_preference >> rail.Label("No") >> is_profile_enabled >> rail.Label("No") >> get_assigned_schedule_policy_empty >> get_assigned_schedule_policy
        get_assigned_schedule_policy >> check_remove_schedule_policy >> rail.Label("No") >> trigger_timeoff_update_for_user
        check_remove_schedule_policy >> rail.Label("Yes") >> remove_schedule_policy >> trigger_timeoff_update_for_user
        is_profile_enabled >> rail.Label("Yes") >> can_update_holiday_calendar

        can_update_holiday_calendar >> rail.Label("No") >> can_update_timeoff_template
        can_update_holiday_calendar >> rail.Label("Yes") >> update_user_holiday_calendar >> can_update_timeoff_template
    
        can_update_timeoff_template >> rail.Label("Yes") >> can_update_timeoff_template_empty >> check_remove_timeoff_template
        can_update_timeoff_template >> rail.Label("No") >> log_exception
    
        check_remove_timeoff_template >> rail.Label("No") >> update_timeoff_template >> log_exception
        check_remove_timeoff_template >> rail.Label("Yes") >> remove_timeoff_template >> log_exception >> get_assigned_schedule_policy_empty

        trigger_timeoff_update_for_user >> wait_for_user_timeoff_update_completion >> supervisor_start 
        supervisor_end >> log_user_success >> should_trigger_delete_time_and_timeoff >> rail.Label("Yes") >> trigger_cleanup_for_disabled_user >> wait_for_trigger_cleanup_for_disabled_user >> catch_and_log_error
        should_trigger_delete_time_and_timeoff >> rail.Label("No") >> catch_and_log_error
        
    return dag

rail.for_each_instance(create_dag)
