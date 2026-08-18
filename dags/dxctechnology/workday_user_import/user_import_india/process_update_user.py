from datetime import timedelta
import json
import pendulum
import rail
from airflow.models import Variable


from dxctechnology.workday_user_import.user_import_india.utils import request_payload, custom_methods
from dxctechnology.workday_user_import.user_import_india.tasks.supervisor_assignment import assign_supervisor
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_todays_date_for_timezone_in_json
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import should_trigger_delete_time_and_timeoff_for_disabled_user
from dxctechnology.workday_user_import.user_import.common_utils.response_filter import get_effective_grp_membership_data_handler

null = None
DATE_FORMAT = "%Y-%d-%m"

# pylint: disable=too-many-statements
def create_update_user_dag(config):
    with rail.create_airflow_dag(
        dag_id = config.india_update_user_dag_id,
        description = "DXC Workday User Import INDIA - Process Update User",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.max_active_run_update_user_india
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_india, default_var='true').lower() == 'true',
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

        is_user_disabled_for_non_go_live_country = rail.IfOperator(
            task_id = "is_user_disabled_for_non_go_live_country",
            test = lambda dag_run:custom_methods.is_user_disabled_for_non_go_live_country(dag_run, get_user_details.task_id),
            yes_task = "get_assigned_permission_for_user",
            no_task = "is_user_already_disabled_41"
        )

        get_assigned_permission_for_user = rail.RepliconServiceOperator(
            task_id="get_assigned_permission_for_user",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        has_no_project_management_permission = rail.IfOperator(
            task_id = "has_no_project_management_permission",
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(
                rail.result("get_assigned_permission_for_user"), "policyUri", "urn:replicon:policy:project-management"
            )),
            yes_task="get_direct_reports_for_user",
            no_task="is_user_already_disabled_41"
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
            test=lambda: not bool(rail.result('get_direct_reports_for_user')),
            yes_task="is_division_gsap",
            no_task="is_user_already_disabled_41"
        )

        def is_division_gsap_test():
            if rail.result("get_effective_group_membership")['parent_division']:
                return rail.result("get_effective_group_membership")['parent_division']['division']['displayText'] == "GSAP"
            return False

        is_division_gsap = rail.IfOperator(
            task_id = "is_division_gsap",
            test=is_division_gsap_test,
            yes_task="is_termination_date_present",
            no_task="disable_user_login_33"
        )

        is_termination_date_present = rail.IfOperator(
            task_id = "is_termination_date_present",
            test="{{dag_run.conf.file_data.term_date | is_truthy}}",
            yes_task="update_user_end_date_15",
            no_task="user_does_not_have_admin_and_payroll_permission"
        )

        update_user_end_date_15 = rail.RepliconServiceOperator(
            task_id = "update_user_end_date_15",
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

        disable_user_login_16 = rail.RepliconServiceOperator(
            task_id = "disable_user_login_16",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        def get_trigger_process_timeoff_policies_items():
            current_timeoff_policies = rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType']

            return list(filter(lambda to_policy:  to_policy['isTimeOffAllowedAgainstThisTimeOffType'] is True
                and bool(to_policy['policySetSchedule'] and to_policy['policySetSchedule'][0]['effectiveDate'].get('day')), current_timeoff_policies))

        trigger_process_timeoff_policies_no_accrual_19 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_process_timeoff_policies_no_accrual_19",
            trigger_dag_id=config.india_process_time_off_no_accrual_dag_id,
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
                "login_name": dag_run.conf['file_data']['email_id'],
                "parent_location" : rail.result("get_effective_group_membership")['parent_location']
            }
        )

        wait_for_process_timeoff_policies_19 = rail.WaitForDagRunsSensor(
            task_id = "wait_for_process_timeoff_policies_19",
            dag_runs="{{result('trigger_process_timeoff_policies_no_accrual_19')}}",
            execution_timeout = timedelta(days=config.execution_timeout_days)
        )

        log_disabled_user_20 =  rail.WriteLogOperator(
            task_id = "log_disabled_user_20",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Sucess",
            properties=lambda dag_run:{
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Success",
                "Details": '''User disabled in Replicon as the required user's company code and country not in allowed status.
                            User's company code is GSAP. User has an end date in the feed file'''
            }
        )


        user_does_not_have_admin_and_payroll_permission = rail.IfOperator(
            task_id= "user_does_not_have_admin_and_payroll_permission",
            test=custom_methods.user_does_not_have_admin_and_payroll_permission_test,
            yes_task="disable_user_login_24",
            no_task="is_user_already_disabled_41"
        )

        disable_user_login_24 = rail.RepliconServiceOperator(
            task_id = "disable_user_login_24",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        can_update_user_end_date_25 = rail.IfOperator(
            task_id = "can_update_user_end_date_25",
            test=custom_methods.can_update_user_end_date_test,
            yes_task="update_user_end_date_26",
            no_task="trigger_process_timeoff_policies_no_accrual_29"
        )

        update_user_end_date_26 = rail.RepliconServiceOperator(
            task_id = "update_user_end_date_26",
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

        trigger_process_timeoff_policies_no_accrual_29 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_process_timeoff_policies_no_accrual_29",
            trigger_dag_id=config.india_process_time_off_no_accrual_dag_id,
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
                "login_name": dag_run.conf['file_data']['email_id'],
                "parent_location" : rail.result("get_effective_group_membership")['parent_location']
            }
        )

        wait_for_process_timeoff_policies_29 = rail.WaitForDagRunsSensor(
            task_id = "wait_for_process_timeoff_policies_29",
            dag_runs="{{result('trigger_process_timeoff_policies_no_accrual_29')}}",
            execution_timeout = timedelta(days=config.execution_timeout_days)
        )

        log_disabled_user_30 =  rail.WriteLogOperator(
            task_id = "log_disabled_user_30",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Sucess",
            properties=lambda dag_run:{
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Success",
                "Details": '''User disabled in Replicon as the required user's company code and country not in allowed status.
                            User's company code is GSAP. User does not have payroll or admin permission'''
            }
        )

        disable_user_login_33 = rail.RepliconServiceOperator(
            task_id = "disable_user_login_33",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        can_update_user_end_date_34 = rail.IfOperator(
            task_id = "can_update_user_end_date_34",
            test=custom_methods.can_update_user_end_date_test,
            yes_task="update_user_end_date_35",
            no_task="trigger_process_timeoff_policies_no_accrual_38"
        )

        update_user_end_date_35 = rail.RepliconServiceOperator(
            task_id = "update_user_end_date_35",
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

        trigger_process_timeoff_policies_no_accrual_38 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_process_timeoff_policies_no_accrual_38",
            trigger_dag_id=config.india_process_time_off_no_accrual_dag_id,
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
                "login_name": dag_run.conf['file_data']['email_id'],
                "parent_location" : rail.result("get_effective_group_membership")['parent_location']
            }
        )

        wait_for_process_timeoff_policies_38 = rail.WaitForDagRunsSensor(
            task_id = "wait_for_process_timeoff_policies_38",
            dag_runs="{{result('trigger_process_timeoff_policies_no_accrual_38')}}",
            execution_timeout = timedelta(days=config.execution_timeout_days)
        )

        log_disabled_user_39 =  rail.WriteLogOperator(
            task_id = "log_disabled_user_39",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Sucess",
            properties=lambda dag_run:{
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Success",
                "Details": '''User disabled in Replicon as the required user's company code and country not in allowed status'''
            }
        )

        is_user_already_disabled_41 = rail.IfOperator(
            task_id = "is_user_already_disabled_41",
            test=custom_methods.is_user_already_disabled_41_test,
            yes_task="can_update_user_end_date_42",
            no_task="is_user_rehire"
        )

        can_update_user_end_date_42 = rail.IfOperator(
            task_id = "can_update_user_end_date_42",
            test=custom_methods.can_update_user_end_date_test,
            yes_task="update_user_end_date_43",
            no_task="log_disabled_user_44"
        )

        update_user_end_date_43 = rail.RepliconServiceOperator(
            task_id = "update_user_end_date_43",
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

        log_disabled_user_44 =  rail.WriteLogOperator(
            task_id = "log_disabled_user_44",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Sucess",
            properties=lambda dag_run:{
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Skipped",
                "Details": '''User already disabled in Replicon'''
            }
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

        update_user_start_date_48 = rail.RepliconServiceOperator(
            task_id = "update_user_start_date_48",
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
            yes_task="update_user_start_date_51",
            no_task="should_disable_user"
        )

        update_user_start_date_51 = rail.RepliconServiceOperator(
            task_id = "update_user_start_date_51",
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
            yes_task="update_user_end_date_53",
            no_task="current_assigned_udf_values"
        )

        update_user_end_date_53 = rail.RepliconServiceOperator(
            task_id = "update_user_end_date_53",
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
            yes_task="disable_user_login_55",
            no_task="current_assigned_udf_values"
        )

        disable_user_login_55 = rail.RepliconServiceOperator(
            task_id = "disable_user_login_55",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        trigger_process_timeoff_policies_no_accrual_58 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_process_timeoff_policies_no_accrual_58",
            trigger_dag_id=config.india_process_time_off_no_accrual_dag_id,
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
                "login_name": dag_run.conf['file_data']['email_id'],
                "parent_location" : rail.result("get_effective_group_membership")['parent_location']
            }
        )

        wait_for_process_timeoff_policies_58 = rail.WaitForDagRunsSensor(
            task_id = "wait_for_process_timeoff_policies_58",
            dag_runs="{{result('trigger_process_timeoff_policies_no_accrual_58')}}",
            execution_timeout = timedelta(days=config.execution_timeout_days)
        )

        log_disabled_user_59 =  rail.WriteLogOperator(
            task_id = "log_disabled_user_59",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Sucess",
            properties=lambda dag_run:{
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Success",
                "Details": '''User disabled in Replicon as "status" is set to 0 for user in feed file'''
            }
        )

        current_assigned_udf_values = rail.PythonOperator(
            task_id = "current_assigned_udf_values",
            python_callable= lambda: custom_methods.get_current_assigned_udf_values(rail.result("get_user_details")['userDetails']['customFieldValues'])
        )

        check_for_is_ia_update =  rail.IfOperator(
            task_id = "check_for_is_ia_update",
            test=lambda dag_run: dag_run.conf['file_data']['is_ia'] and dag_run.conf['file_data']['is_ia'] !=
                rail.result("current_assigned_udf_values")['is_ia'],
            yes_task="is_log_for_ia_exception_applicable",
            no_task="get_time_entry_approval_path_name"
        )

        is_log_for_ia_exception_applicable =  rail.IfOperator(
            task_id = "is_log_for_ia_exception_applicable",
            test=request_payload.validate_log_for_ia_exception_applicable,
            yes_task="log_ia_exception",
            no_task="get_time_entry_approval_path_name"
        )

        log_ia_exception =  rail.WriteLogOperator(
            task_id = "log_ia_exception",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Exception",
            properties=lambda dag_run:{
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Exception",
                "Details": request_payload.get_ia_exception_message(dag_run)
            }
        )

        get_time_entry_approval_path_name = rail.RepliconServiceOperator(
            task_id = "get_time_entry_approval_path_name",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/GetApprovalPathForUser",
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

        is_timesheet_template_removal_required_233 =  rail.IfOperator(
            task_id = "is_timesheet_template_removal_required_233",
            test=lambda dag_run: dag_run.conf['mapper_data']['profile_status']=="enabled" and dag_run.conf['mapper_data']['timesheet_template_name']
            and rail.result("get_user_details")['timesheetTemplate'] and rail.result("get_user_details")['timesheetTemplate']['name'],
            yes_task="is_mgmnt_lvl_1_2",
            no_task="update_user"
        )

        is_mgmnt_lvl_1_2 =  rail.IfOperator(
            task_id = "is_mgmnt_lvl_1_2",
            test=lambda dag_run: dag_run.conf['file_data']['management_lvl'] in ['L1','L2'],
            yes_task="remove_timesheet_template",
            no_task="update_user"
        )

        remove_timesheet_template = rail.RepliconServiceOperator(
            task_id = "remove_timesheet_template",
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data=lambda dag_run:{
                "userUri" :  dag_run.conf['user_uri'],
                "policySetUri": rail.result("get_user_details")['timesheetTemplate']['uri']
            }
        )

        update_user = rail.RepliconServiceOperator(
            task_id = "update_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data = lambda dag_run: request_payload.get_update_user_payload(dag_run, config)
        )

        can_update_notification_preference = rail.IfOperator(
            task_id = "can_update_notification_preference",
            test="{{ dag_run.conf.file_data.management_lvl in ['L1', 'L2']}}",
            yes_task="update_notification_preference",
            no_task="dummy_process_supervisor"
        )

        update_notification_preference = rail.RepliconServiceOperator(
            task_id = "update_notification_preference",
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data=lambda dag_run: request_payload.get_notification_preference_to_assign(dag_run, "update")
        )

        dummy_process_supervisor = rail.EmptyOperator(
            task_id="dummy_process_supervisor"
        )

        start_supervisor_update, end_supervisor_update = assign_supervisor("update_supervisor", "update")

        process_update_user_timeoff_assignment = rail.TriggerDagRunOperator(
            task_id = "process_update_user_timeoff_assignment",
            trigger_dag_id=config.india_update_user_timeoff_assignment_dag_id,
            conf=lambda dag_run:{
                "file_name": dag_run.conf['master_file_name'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": rail.result('update_user')['loginName'],
                "company_code": dag_run.conf['file_data']['company_code'],
                "source": dag_run.conf['mapper_data']['parent_company'],
                "country": dag_run.conf['file_data']['country'],
                'rehire': "Yes" if custom_methods.is_user_rehire_test(dag_run) else "No",
                "starting_balance_set_to_uri":dag_run.conf['starting_balance_set_to_uri'],
                "prevent_balance_overdraw_uri":dag_run.conf['prevent_balance_overdraw_uri'],
                "time_type": dag_run.conf['file_data']['time_type'],
                "continuous_service_date": dag_run.conf['file_data']['service_date'],
                "continuous_service_date_json_format": dag_run.conf['json_formatted_dates']['service_date'],
                "gender":  dag_run.conf['file_data']['gender'] if dag_run.conf['file_data']['gender']=="Male" else "Others",
                "dob":dag_run.conf['file_data']['dob'],
                "dob_json_format": dag_run.conf['json_formatted_dates']['date_of_birth'],
                "start_date": dag_run.conf['file_data']['hire_date'],
                "start_date_json_format": dag_run.conf['json_formatted_dates']['hire_date'],
                "todays_date":(pendulum.now('America/Los_Angeles')).strftime(DATE_FORMAT),
                "todays_json_format": get_todays_date_for_timezone_in_json(),
                "job_level": dag_run.conf['job_level_set'],
                "is_ia": dag_run.conf['file_data']['is_ia'],
                "ia_start_date":dag_run.conf['file_data']['ia_start_date'],
                "ia_start_date_json_format": dag_run.conf['json_formatted_dates']['ia_start_date'],
                "ia_end_date":dag_run.conf['file_data']['ia_end_date'],
                "ia_end_date_json_format": dag_run.conf['json_formatted_dates']['ia_end_date'],
                "is_ia_update":"Yes" if dag_run.conf['file_data']['is_ia'] and dag_run.conf['file_data']['is_ia'] !=
                    rail.result("current_assigned_udf_values")['is_ia'] else "No",
                "assignment_type": dag_run.conf['file_data']['assignment_type']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_update_user_timeoff_assignment = rail.WaitForDagRunsSensor(
            task_id = "wait_for_update_user_timeoff_assignment",
            dag_runs="""{{ result('process_update_user_timeoff_assignment') }}""",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )


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

        get_user_details >> get_effective_group_membership >> get_user_timeoff_policy_summary
        get_user_timeoff_policy_summary >> is_user_disabled_for_non_go_live_country >> rail.Label("No") >> is_user_already_disabled_41

        is_user_already_disabled_41 >> rail.Label("No") >> is_user_rehire
        is_user_already_disabled_41 >> rail.Label("Yes") >> can_update_user_end_date_42 >> rail.Label("Yes") >> update_user_end_date_43 >> log_disabled_user_44
        can_update_user_end_date_42 >> rail.Label("No") >> log_disabled_user_44 >> rail.Label("No") >> catch_and_log_error


        is_user_disabled_for_non_go_live_country >> rail.Label("Yes") >> get_assigned_permission_for_user >> has_no_project_management_permission
        has_no_project_management_permission >> rail.Label("Yes") >> get_direct_reports_for_user >> has_no_direct_reports_for_user
        has_no_project_management_permission >> rail.Label("No") >> is_user_already_disabled_41

        has_no_direct_reports_for_user >> rail.Label('No') >> is_user_already_disabled_41
        has_no_direct_reports_for_user >> rail.Label('Yes') >> is_division_gsap

        is_division_gsap >> rail.Label('Yes') >> is_termination_date_present >> rail.Label('Yes') >> update_user_end_date_15
        update_user_end_date_15 >> disable_user_login_16 >> trigger_process_timeoff_policies_no_accrual_19 >> wait_for_process_timeoff_policies_19
        wait_for_process_timeoff_policies_19 >> log_disabled_user_20 >> rail.Label("No") >> catch_and_log_error

        is_termination_date_present >> rail.Label('No') >> user_does_not_have_admin_and_payroll_permission
        user_does_not_have_admin_and_payroll_permission >> rail.Label('Yes') >> disable_user_login_24 >> can_update_user_end_date_25
        user_does_not_have_admin_and_payroll_permission >> rail.Label('No') >> is_user_already_disabled_41

        can_update_user_end_date_25 >> rail.Label('No') >> trigger_process_timeoff_policies_no_accrual_29
        can_update_user_end_date_25 >> rail.Label('Yes') >> update_user_end_date_26 >> trigger_process_timeoff_policies_no_accrual_29
        trigger_process_timeoff_policies_no_accrual_29 >> wait_for_process_timeoff_policies_29 >> log_disabled_user_30 >> rail.Label("No") >> catch_and_log_error

        is_division_gsap >> rail.Label('No') >> disable_user_login_33 >> can_update_user_end_date_34
        can_update_user_end_date_34 >> rail.Label("No") >> trigger_process_timeoff_policies_no_accrual_38
        can_update_user_end_date_34 >> rail.Label("Yes") >> update_user_end_date_35 >> trigger_process_timeoff_policies_no_accrual_38
        trigger_process_timeoff_policies_no_accrual_38 >> wait_for_process_timeoff_policies_38 >> log_disabled_user_39
        log_disabled_user_39 >> rail.Label("No") >> catch_and_log_error

        is_user_rehire >> rail.Label("Yes") >> enable_login >> update_user_start_date_48 >> can_update_user_start_date
        is_user_rehire >> rail.Label("No") >> can_update_user_start_date >> rail.Label('No') >> should_disable_user
        can_update_user_start_date >> rail.Label("Yes") >> update_user_start_date_51 >> should_disable_user

        should_disable_user >> rail.Label('Yes') >> update_user_end_date_53 >> is_end_date_less_than_today >> rail.Label("No") >> current_assigned_udf_values

        is_end_date_less_than_today >> rail.Label('Yes') >> disable_user_login_55 >> trigger_process_timeoff_policies_no_accrual_58
        trigger_process_timeoff_policies_no_accrual_58 >> wait_for_process_timeoff_policies_58 >> log_disabled_user_59 >> rail.Label("No") >> catch_and_log_error
        should_disable_user >> rail.Label('No') >> current_assigned_udf_values

        current_assigned_udf_values >> check_for_is_ia_update >> rail.Label('No') >> get_time_entry_approval_path_name

        check_for_is_ia_update >> rail.Label('No') >> get_time_entry_approval_path_name
        check_for_is_ia_update >> rail.Label('Yes') >> is_log_for_ia_exception_applicable >> rail.Label('Yes') >> log_ia_exception >> rail.Label("No") >> catch_and_log_error
        is_log_for_ia_exception_applicable >> rail.Label('No') >> get_time_entry_approval_path_name

        get_time_entry_approval_path_name >> get_user_assigned_policy >> is_timesheet_template_removal_required_233
        is_timesheet_template_removal_required_233 >> rail.Label('Yes') >> is_mgmnt_lvl_1_2 >>rail.Label('Yes') >> remove_timesheet_template
        is_mgmnt_lvl_1_2 >>rail.Label('No') >> update_user
        remove_timesheet_template >> update_user
        is_timesheet_template_removal_required_233 >> rail.Label('No') >> update_user >> can_update_notification_preference

        can_update_notification_preference >> rail.Label("Yes") >> update_notification_preference >> dummy_process_supervisor
        can_update_notification_preference >> rail.Label('No') >> dummy_process_supervisor

        dummy_process_supervisor >> start_supervisor_update >> end_supervisor_update
        end_supervisor_update >> process_update_user_timeoff_assignment >> wait_for_update_user_timeoff_assignment
        wait_for_update_user_timeoff_assignment >> log_user_completion >> rail.Label("No") >> catch_and_log_error

        return dag

rail.for_each_instance(create_update_user_dag)
