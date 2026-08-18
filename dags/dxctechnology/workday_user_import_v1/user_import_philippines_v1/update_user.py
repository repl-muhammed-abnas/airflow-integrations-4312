from datetime import timedelta
from json import dumps
import pendulum
import rail
from airflow.models import Variable


from dxctechnology.workday_user_import_v1.user_import_philippines_v1.utils import request_payload, custom_methods
from dxctechnology.workday_user_import_v1.user_import_philippines_v1.tasks.supervisor_assignment import assign_supervisor
from dxctechnology.workday_user_import_v1.user_import_philippines_v1.utils.response_filter import get_effective_grp_membership_data_handler
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import should_trigger_delete_time_and_timeoff_for_disabled_user
from dxctechnology.workday_user_import_v1.user_import_philippines_v1.utils.request_payload import get_todays_date_for_timezone_in_json

null = None

# pylint: disable=too-many-statements
def create_update_user_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id = f"{config.workday_user_import_philippines_update_user_dag}{prefix}",
            description = config.workday_user_import_philippines_update_user_dag_description,
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs = config.max_active_run_update_user_philippines
        ) as dag:

            rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id = "can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name_philippines, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="get_user_details"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id = "batch_task",
                start_task="get_user_details",
                end_task="catch_and_log_error",
                execution_timeout=timedelta(days=14)
            )

            def get_users_data(dag_run):
                # Check if user_uri exists in dag_run.conf
                if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf or not dag_run.conf.get('user_uri'):
                    raise ValueError("Missing user_uri in dag_run.conf")

                return {
                    "users": [
                        {
                            "uri": dag_run.conf['user_uri'],
                            "loginName": null,
                            "parameterCorrelationId": null
                        }
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                }

            get_user_details = rail.RepliconServiceOperator(
                task_id = "get_user_details",
                endpoint = "/services/ImportService1.svc/BulkGetUsers3",
                data = get_users_data,
                data_handler=lambda response: response[0] if response else {}
            )

            get_user_details_2 = rail.RepliconServiceOperator(
                task_id = "get_user_details_2",
                endpoint = "/services/ImportService2.svc/GetUserDetails",
                data = lambda dag_run: {
                    "user": {
                        "uri": dag_run.conf['user_uri'],
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    },
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                },
                data_handler=lambda response: response if response else {}
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

            # this logic will never go to the yes_task in integration runs, as the profile status will always be enabled
            # added this for reassurance, still this code will be never be executed unless a manual run is triggered
            # with the modified config
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

            is_division_gsap = rail.IfOperator(
                task_id = "is_division_gsap",
                test=custom_methods.is_division_gsap_test,
                yes_task="is_termination_date_present",
                no_task="disable_user_login_33"
            )

            def is_term_date_present(dag_run):
                if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf:
                    return False

                file_data = dag_run.conf.get('file_data', {})
                return bool(file_data.get('term_date'))

            is_termination_date_present = rail.IfOperator(
                task_id = "is_termination_date_present",
                test=is_term_date_present,
                yes_task="update_user_end_date_15",
                no_task="user_does_not_have_admin_and_payroll_permission"
            )

            update_user_end_date_15 = rail.RepliconServiceOperator(
                task_id = "update_user_end_date_15",
                endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
                data=request_payload.get_user_end_date_update_payload_15
            )

            disable_user_login_16 = rail.RepliconServiceOperator(
                task_id = "disable_user_login_16",
                endpoint="/services/SecurityService1.svc/DisableLogin",
                data={
                    "userUri": "{{dag_run.conf.user_uri}}"
                }
            )

            trigger_process_timeoff_policies_no_accrual_19 = rail.TriggerDagRunForEachItemOperator(
                task_id = "trigger_process_timeoff_policies_no_accrual_19",
                trigger_dag_id=lambda dag_run: custom_methods.get_trigger_dag_id(
                    config.process_time_off_accrual,
                    config.DAG_BATCH_COUNT,
                    item_index=(custom_methods.get_item_index(dag_run, config.DAG_BATCH_COUNT))
                ),
                items=custom_methods.get_trigger_process_timeoff_policies_items,
                conf=lambda item, dag_run: {
                    "file_name": dag_run.conf["file_name"],
                    "user_uri": dag_run.conf["user_uri"],
                    "timeoff_type_uri": item['timeOffType']["uri"],
                    "policy_set": dumps(item['policySetSchedule']).replace("[[{", "[{").replace("}]]", "}]"),
                    "end_date": dag_run.conf['file_data']['term_date'],
                    "user_end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
                    "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                    "prevent_balance_overdraw_uri": dag_run.conf["prevent_balance_overdraw_uri"],
                    "login_name": dag_run.conf['file_data']['email_id'],
                    "parent_location" : rail.result("get_effective_group_membership")['parent_location'],
                    "add_balance_as_zero": "yes"
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
                data=request_payload.get_user_end_date_update_payload_15
            )

            trigger_process_timeoff_policies_no_accrual_29 = rail.TriggerDagRunForEachItemOperator(
                task_id = "trigger_process_timeoff_policies_no_accrual_29",
                trigger_dag_id=lambda dag_run: custom_methods.get_trigger_dag_id(
                    config.process_time_off_accrual,
                    config.DAG_BATCH_COUNT,
                    item_index=(custom_methods.get_item_index(dag_run, config.DAG_BATCH_COUNT))
                ),
                items=custom_methods.get_trigger_process_timeoff_policies_items,
                conf=lambda item, dag_run: {
                    "file_name": dag_run.conf["file_name"],
                    "user_uri": dag_run.conf["user_uri"],
                    "timeoff_type_uri": item['timeOffType']["uri"],
                    "policy_set": dumps(item['policySetSchedule']).replace("[[{", "[{").replace("}]]", "}]"),
                    "end_date": dag_run.conf['file_data']['term_date'],
                    "user_end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
                    "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                    "prevent_balance_overdraw_uri": dag_run.conf["prevent_balance_overdraw_uri"],
                    "login_name": dag_run.conf['file_data']['email_id'],
                    "parent_location" : rail.result("get_effective_group_membership")['parent_location'],
                    "add_balance_as_zero": "yes"
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
                trigger_dag_id=lambda dag_run: custom_methods.get_trigger_dag_id(
                    config.process_time_off_accrual,
                    config.DAG_BATCH_COUNT,
                    item_index=(custom_methods.get_item_index(dag_run, config.DAG_BATCH_COUNT))
                ),
                items=custom_methods.get_trigger_process_timeoff_policies_items,
                conf=lambda item, dag_run: {
                    "file_name": dag_run.conf["file_name"],
                    "user_uri": dag_run.conf["user_uri"],
                    "timeoff_type_uri": item['timeOffType']["uri"],
                    "policy_set": dumps(item['policySetSchedule']).replace("[[{", "[{").replace("}]]", "}]"),
                    "end_date": dag_run.conf['file_data']['term_date'],
                    "user_end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
                    "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                    "prevent_balance_overdraw_uri": dag_run.conf["prevent_balance_overdraw_uri"],
                    "login_name": dag_run.conf['file_data']['email_id'],
                    "parent_location" : rail.result("get_effective_group_membership")['parent_location'],
                    "add_balance_as_zero": "yes"
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
                test=custom_methods.is_user_already_disabled_test,
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
                severity="Skipped",
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
                data=request_payload.update_user_start_date_remove_end_date
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
                data=request_payload.update_user_start_date_remove_end_date
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
                data=request_payload.get_user_end_date_update_payload_15
            )

            is_end_date_less_than_today = rail.IfOperator(
                task_id = "is_end_date_less_than_today",
                test=custom_methods.is_end_date_less_than_today_test,
                yes_task="disable_user_login_55",
                no_task="no_task_is_end_date_less_than_today"
            )

            no_task_is_end_date_less_than_today = rail.EmptyOperator(
                task_id = "no_task_is_end_date_less_than_today"
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
                trigger_dag_id=lambda dag_run: custom_methods.get_trigger_dag_id(
                    config.process_time_off_accrual,
                    config.DAG_BATCH_COUNT,
                    item_index=(custom_methods.get_item_index(dag_run, config.DAG_BATCH_COUNT))
                ),
                items=custom_methods.get_trigger_process_timeoff_policies_items,
                conf=lambda item, dag_run: {
                    "file_name": dag_run.conf["file_name"],
                    "user_uri": dag_run.conf["user_uri"],
                    "timeoff_type_uri": item['timeOffType']["uri"],
                    "policy_set": dumps(item['policySetSchedule']).replace("[[{", "[{").replace("}]]", "}]"),
                    "end_date": custom_methods.convert_json_date_to_string_date(custom_methods.get_specified_json_date_minus_specified_days_months_years_date_in_json(dag_run.conf['json_formatted_dates']['term_date'], days_in_number=-1)),
                    "user_end_date_json": custom_methods.get_specified_json_date_minus_specified_days_months_years_date_in_json(dag_run.conf['json_formatted_dates']['term_date'], days_in_number=-1),
                    "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                    "prevent_balance_overdraw_uri": dag_run.conf["prevent_balance_overdraw_uri"],
                    "login_name": dag_run.conf['file_data']['email_id'],
                    "parent_location" : rail.result("get_effective_group_membership")['parent_location'],
                    "add_balance_as_zero": "yes"
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

            # this needs to be updated
            current_assigned_udf_values = rail.PythonOperator(
                task_id = "current_assigned_udf_values",
                python_callable= lambda: custom_methods.get_current_assigned_udf_values(rail.result("get_user_details")['userDetails']['customFieldValues'])
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
                data = lambda : rail.result("prepare_update_payload")
            )

            can_update_timesheet_template = rail.IfOperator(
                task_id = "can_update_timesheet_template",
                test=lambda : rail.result('prepare_update_payload', 'timesheet_template_update'),
                yes_task="update_timesheet_template_with_effective_date",
                no_task="update_notification_preference"
            )

            update_timesheet_template_with_effective_date = rail.RepliconServiceOperator(
                task_id = "update_timesheet_template_with_effective_date",
                endpoint = "/services/ImportService1.svc/ApplyUserModifications3",
                data = lambda dag_run: request_payload._get_update_timesheet_template_update_payload(dag_run, "update")
            )

            update_notification_preference = rail.EmptyOperator(
                task_id = "update_notification_preference"
            )

            dummy_process_supervisor = rail.EmptyOperator(
                task_id="dummy_process_supervisor"
            )

            start_supervisor_update, end_supervisor_update = assign_supervisor("update_supervisor", "update")

            get_timeoff_data_from_mapper = rail.PythonOperator(
                task_id = "get_timeoff_data_from_mapper",
                python_callable= lambda dag_run: custom_methods.get_mapper_timeoff_data(dag_run, config.TIMEOFF_MAPPER)
            )

            has_any_data = rail.IfOperator(
                task_id = "has_any_data",
                test=lambda : len(rail.result("get_timeoff_data_from_mapper")) > 0,
                yes_task="get_all_timeoffs",
                no_task="log_user_completion"
            )

            get_all_timeoffs = rail.RepliconServiceOperator(
                task_id = "get_all_timeoffs",
                endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
            )

            get_assigned_timeoff_types = rail.PythonOperator(
                task_id = "get_assigned_timeoff_types",
                python_callable= lambda: custom_methods.get_filtered_user_timeoff_policy(rail.result("get_user_timeoff_policy_summary"))
            )


            is_user_rehire_timeoff = rail.IfOperator(
                task_id = "is_user_rehire_timeoff",
                test=lambda: rail.result("is_user_rehire", 'rehire').lower() == "yes",
                yes_task="trigger_rehire_timeoff_assignment",
                no_task="get_required_timeoff_type_details"
            )

            def get_rehire_timeoff_types():
                return [row for row in rail.result("get_assigned_timeoff_types") if row['policy']]

            def get_json_conf():
                dag_run_conf = rail.get_dag_run_conf()
                return rail.write_json_artifact(dag_run_conf)

            trigger_rehire_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
                task_id = "trigger_rehire_timeoff_assignment",
                trigger_dag_id=lambda dag_run: custom_methods.get_trigger_dag_id(
                    config.workday_user_import_philippines_user_rehire_timeoff_process_dag,
                    config.DAG_BATCH_COUNT,
                    item_index=(custom_methods.get_item_index(dag_run, config.DAG_BATCH_COUNT))
                ),
                items=get_rehire_timeoff_types,
                conf= lambda dag_run, item : {
                        "timeoff_type_uri": item['uri'],
                        "current_timeoff_policies": item['policy'],
                        "timeoff_type_name": item['name'],
                        "json_formatted_dates": {
                            "start_date": request_payload.get_todays_date_in_json(),
                            "continuous_service_date": dag_run.conf['json_formatted_dates']['service_date']
                        },
                        "user_uri":  dag_run.conf['user_uri'],
                        "user_log": dag_run.conf['user_log'],
                        "emp_id": dag_run.conf['file_data']['emp_id'],
                        "email_id": dag_run.conf['file_data']['email_id'],
                        "other_data": get_json_conf(),
                        "fte": dag_run.conf['file_data']['fte']
                },
                retries= 0,
                execution_timeout = timedelta(days=1)
            )

            wait_for_trigger_rehire_timeoff_assignment = rail.WaitForDagRunsSensor(
                task_id = "wait_for_trigger_rehire_timeoff_assignment",
                dag_runs="{{result('trigger_rehire_timeoff_assignment')}}",
                execution_timeout = timedelta(days=config.execution_timeout_days)
            )

            def timeoff_assignment_data():
                replicon_timeoffs = rail.result("get_all_timeoffs")
                mapper_timeoffs = rail.result('get_timeoff_data_from_mapper')

                timeoff_list =  list(map(lambda timeoff: {
                        "name": timeoff['Timeoff Type Name'],
                        "uri": rail.find_first_by_attr_and_get_attr(
                            replicon_timeoffs, 'name', timeoff['Timeoff Type Name'].strip(), 'uri'),
                        "mapper_data": timeoff
                    }, mapper_timeoffs))

                filtered_timeoff_list = list(filter(lambda x: bool(x['uri']), timeoff_list))
                timeoff_unique_uri_list_to_assign = [ fto['uri'] for fto in filtered_timeoff_list]
                timeoff_to_disable_after_assignment = [ fto for fto in filtered_timeoff_list if fto['mapper_data']['Should Disabled After Assignment'].lower() != "yes"]
                return {
                    "timeoff_unique_uri_list_to_assign": timeoff_unique_uri_list_to_assign,
                    "timeoff_list_mapped_as_per_replicon": timeoff_list,
                    "timeoff_list_to_assign": filtered_timeoff_list,
                    "formatted_timeoff_uri_list_to_assign": [{"timeoff_uri": item } for item in timeoff_unique_uri_list_to_assign],
                    "timeoff_to_disable_after_assignment": timeoff_to_disable_after_assignment
                }

            def get_required_details_callable():
                _timeoff_assignment_data = timeoff_assignment_data()
                rail.set_result(key="timeoff_assignment_data", val= _timeoff_assignment_data)
                final_timeoff_list = _timeoff_assignment_data["formatted_timeoff_uri_list_to_assign"]
                timeoff_list_as_per_mapper = _timeoff_assignment_data['timeoff_list_mapped_as_per_replicon']
                current_assigned_timeoffs = rail.result("get_assigned_timeoff_types")
                timeoff_to_disable_after_assignment = _timeoff_assignment_data['timeoff_to_disable_after_assignment']

                timeoffs_to_assign = list(filter(lambda _timeoff: _timeoff['status'] == 'No',map(
                        lambda timeoff: {
                            "name": rail.find_first_by_attr_and_get_attr(timeoff_list_as_per_mapper,'uri',timeoff['timeoff_uri'],'name'),
                            "enabled":rail.find_first_by_attr_and_get_attr(current_assigned_timeoffs,'uri',timeoff['timeoff_uri'],'enabled'),
                            "uri": timeoff['timeoff_uri'],
                            "policy": rail.find_first_by_attr_and_get_attr(current_assigned_timeoffs,'uri',timeoff['timeoff_uri'],'policy', default=[]),
                            "status":"Yes" if rail.find_first_by_attr_and_get_attr(current_assigned_timeoffs,'uri',timeoff['timeoff_uri'],'name') else "No",
                            "timeoff_to_disable_after_assignment": timeoff['timeoff_uri'] in timeoff_to_disable_after_assignment,
                            "mapper_data": rail.find_first_by_attr_and_get_attr(timeoff_list_as_per_mapper,'uri',timeoff['timeoff_uri'],'mapper_data', default={}),
                            # Check if timeoff exists but is disabled (for remarriage scenario)
                            "is_currently_disabled": rail.find_first_by_attr_and_get_attr(current_assigned_timeoffs,'uri',timeoff['timeoff_uri'],'enabled') == False if rail.find_first_by_attr_and_get_attr(current_assigned_timeoffs,'uri',timeoff['timeoff_uri'],'name') else False
                        }
                    ,final_timeoff_list)))

                timeoffs_to_disable = list(filter(lambda _timeoff: _timeoff['status'] == 'No',map(lambda timeoff: {
                    "name": timeoff['name'],
                    "uri": timeoff['uri'],
                    "enabled": timeoff['enabled'],
                    "policy": timeoff['policy'],
                    "status": "Yes" if rail.find_first_by_attr_and_get_attr(final_timeoff_list,"timeoff_uri",timeoff['uri']) else "No"
                }, current_assigned_timeoffs)))

                return {
                    "final_time_off_list": final_timeoff_list,
                    "timeoffs_to_assign": [ {**{"to_index": timeoff_index}, **_timeoff} for timeoff_index, _timeoff in enumerate(timeoffs_to_assign)],
                    "timeoffs_to_disable": [ {**{"to_index": disable_timeoff_index}, **_disable_timeoff} for disable_timeoff_index, _disable_timeoff in enumerate(timeoffs_to_disable)]
                }

            get_required_timeoff_type_details = rail.PythonOperator(
                task_id = "get_required_timeoff_type_details",
                python_callable = get_required_details_callable
            )


            has_any_timeoff_to_assign_or_disable = rail.IfOperator(
                task_id = "has_any_timeoff_to_assign_or_disable",
                test=lambda: bool(rail.result("get_required_timeoff_type_details")['timeoffs_to_assign']) or bool(rail.result("get_required_timeoff_type_details")['timeoffs_to_disable']),
                yes_task="assign_timeoff_to_user",
                no_task="log_user_completion"
            )

            assign_timeoff_to_user = rail.RepliconServiceOperator(
                task_id="assign_timeoff_to_user",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=lambda dag_run :{
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUris":[to['uri'] for to in rail.result("get_required_timeoff_type_details")['timeoffs_to_assign']]
                }
            )

            has_any_timeoff_to_disable = rail.IfOperator(
                task_id = "has_any_timeoff_to_disable",
                test=lambda : len(rail.result('get_required_timeoff_type_details')['timeoffs_to_disable']) > 0,
                yes_task="process_timeoff_no_accrual",
                no_task="has_any_timeoff_types_to_assign"
            )

            def get_filtered_timeoff_types_to_disable():
                return [row for row in rail.result("get_required_timeoff_type_details")["timeoffs_to_disable"] if row['policy']]

            process_timeoff_no_accrual = rail.TriggerDagRunForEachItemOperator(
                task_id="process_timeoff_no_accrual",
                items=get_filtered_timeoff_types_to_disable,
                trigger_dag_id=lambda dag_run, item: custom_methods.get_trigger_dag_id(
                    config.process_time_off_accrual,
                    config.DAG_BATCH_COUNT,
                    item_index=item['to_index']#(custom_methods.get_item_index(dag_run, config.DAG_BATCH_COUNT))
                ),
                    conf=lambda dag_run, item: {
                    **dag_run.conf,
                    **{
                        "timeoff_type_uri": item['uri'],
                        "policy_set": dumps(item['policy']).replace("[[{", "[{").replace("}]]", "}]"),
                        "today": custom_methods.get_todays_date_in_json(),
                        "user_end_date_json": custom_methods.date_to_use_for_disable(dag_run, return_as_json_date=True),
                        "end_date": custom_methods.date_to_use_for_disable(dag_run, return_as_json_date=False)
                    }
                },
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            wait_for_process_timeoff_no_accrual = rail.WaitForDagRunsSensor(
                task_id="wait_for_process_timeoff_no_accrual",
                dag_runs="{{result('process_timeoff_no_accrual')}}",
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            has_any_timeoff_types_to_assign = rail.IfOperator(
                task_id = "has_any_timeoff_types_to_assign",
                test=lambda: bool(rail.result("get_required_timeoff_type_details")['timeoffs_to_assign']),
                yes_task="process_timeoffs",
                no_task="log_user_completion"
            )

            process_timeoffs = rail.TriggerDagRunForEachItemOperator(
                    task_id="process_timeoffs",
                    items=lambda: rail.result("get_required_timeoff_type_details")['timeoffs_to_assign'],
                    trigger_dag_id=lambda dag_run, item: custom_methods.get_trigger_dag_id(
                        config.workday_user_import_philippines_update_user_timeoff_assignment_dag,
                        config.DAG_BATCH_COUNT,
                        item_index=item['to_index']#(custom_methods.get_item_index(dag_run, config.DAG_BATCH_COUNT))
                    ),
                    conf=lambda dag_run, item: {
                    **{
                    "feed_file_name": dag_run.conf["file_name"],
                        "user_log_name": dag_run.conf['user_log'],
                        "emp_id": dag_run.conf['file_data']["emp_id"],
                        "email_id": dag_run.conf['file_data']["email_id"],
                        "loginName": rail.result('update_user')['loginName'],
                        "start_date": custom_methods.get_todays_date_in_json(),
                        "Contineousservicedate": dag_run.conf['json_formatted_dates']['service_date'],
                        "timetype": dag_run.conf['file_data']['time_type'],
                        "gender": dag_run.conf['file_data']['gender'],
                        "end_date": dag_run.conf['file_data']['term_date'],
                        "end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
                        "company_code": dag_run.conf['file_data']['company_code'],
                        "parent_company_code": dag_run.conf['file_data']['parent_company'],
                        "country": dag_run.conf['file_data']['country'],
                        "parent_location" : rail.result("get_effective_group_membership")['parent_location'],
                        "ia_updated": rail.result("prepare_update_payload", "ia_updated"),
                        "ia_end_date": dag_run.conf['json_formatted_dates']['ia_end_date'],
                        "ia_start_date": dag_run.conf['json_formatted_dates']['ia_start_date'],
                        "assignment_type": dag_run.conf['file_data']['assignment_type'],
                        "timeoff_type_uri": item['uri'],
                        "timeoff_type_details": item
                    },
                    **dag_run.conf
                    },
                    execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            wait_for_process_timeoffs = rail.WaitForDagRunsSensor(
                    task_id="wait_for_process_timeoffs",
                    dag_runs="{{result('process_timeoffs')}}",
                    execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            reassign_timeoff_to_user = rail.RepliconServiceOperator(
                task_id="reassign_timeoff_to_user",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=lambda dag_run :{
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUris": [timeoff['uri'] for timeoff in rail.result("get_required_timeoff_type_details", "timeoff_assignment_data")['timeoff_to_disable_after_assignment']]
                }
            )

            def get_log_message():
                exception_msg = rail.result('prepare_update_payload', 'exception_log')
                if exception_msg:
                    return f"User updated partially - {rail.smartjoin_by_delim(exception_msg, ',')}"
                return "User updated successfully"

            def get_update_completion_log_properties(dag_run):
                # Check if we have valid dag_run.conf and file_data
                if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf:
                    return {
                        "Jobid": "",
                        "Userid": "Unknown",
                        "Email": "Unknown",
                        "Action": "Update",
                        "Status": "Unknown",
                        "Details": "Missing dag_run.conf data"
                    }

                file_data = dag_run.conf.get('file_data', {})
                emp_id = file_data.get('emp_id', 'Unknown')
                email_id = file_data.get('email_id', 'Unknown')
                status = "Exception" if bool(rail.result('prepare_update_payload', 'exception_log')) else "Success"

                return {
                    # WriteLogOperator ecid has ecid | run_id
                    "Jobid": "",
                    "Userid": emp_id,
                    "Email": email_id,
                    "Action": "Update",
                    "Status": status,
                    "Details": get_log_message()
                }

            log_user_completion = rail.WriteLogOperator(
                task_id = "log_user_completion",
                message = "User Update",
                log="{{dag_run.conf.user_log}}",
                severity = "Success",
                properties = get_update_completion_log_properties
            )

            def get_update_user_error_log_properties(dag_run):
                # Check if we have valid dag_run.conf and file_data
                if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf:
                    return {
                        "Jobid": "",
                        "Userid": "Unknown",
                        "Email": "Unknown",
                        "Action": "Update",
                        "Status": "Error",
                        "Details": "Missing dag_run.conf data"
                    }

                file_data = dag_run.conf.get('file_data', {})
                emp_id = file_data.get('emp_id', 'Unknown')
                email_id = file_data.get('email_id', 'Unknown')
                error_message = rail.render_template("{{get_error_message()}}")

                return {
                    "Jobid": "",
                    "Userid": emp_id,
                    "Email": email_id,
                    "Action": "Update",
                    "Status": "Error",
                    "Details": error_message
                }

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
                properties=get_update_user_error_log_properties
            )

            can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
            can_run_batch_task >> rail.Label("No") >> get_user_details

            get_user_details >> get_user_details_2 >> get_effective_group_membership >> get_user_timeoff_policy_summary
            get_user_timeoff_policy_summary >> is_user_disabled_for_non_go_live_country >> rail.Label("No") >> is_user_already_disabled_41

            is_user_already_disabled_41 >> rail.Label("No") >> is_user_rehire
            is_user_already_disabled_41 >> rail.Label("Yes") >> can_update_user_end_date_42 >> rail.Label("Yes") >> update_user_end_date_43 >> log_disabled_user_44
            can_update_user_end_date_42 >> rail.Label("No") >> log_disabled_user_44 >> should_trigger_delete_time_and_timeoff


            is_user_disabled_for_non_go_live_country >> rail.Label("Yes") >> get_assigned_permission_for_user >> has_no_project_management_permission
            has_no_project_management_permission >> rail.Label("Yes") >> get_direct_reports_for_user >> has_no_direct_reports_for_user
            has_no_project_management_permission >> rail.Label("No") >> is_user_already_disabled_41

            has_no_direct_reports_for_user >> rail.Label('No') >> is_user_already_disabled_41
            has_no_direct_reports_for_user >> rail.Label('Yes') >> is_division_gsap

            is_division_gsap >> rail.Label('Yes') >> is_termination_date_present >> rail.Label('Yes') >> update_user_end_date_15
            update_user_end_date_15 >> disable_user_login_16 >> trigger_process_timeoff_policies_no_accrual_19 >> wait_for_process_timeoff_policies_19
            wait_for_process_timeoff_policies_19 >> log_disabled_user_20 >> should_trigger_delete_time_and_timeoff

            is_termination_date_present >> rail.Label('No') >> user_does_not_have_admin_and_payroll_permission
            user_does_not_have_admin_and_payroll_permission >> rail.Label('Yes') >> disable_user_login_24 >> can_update_user_end_date_25
            user_does_not_have_admin_and_payroll_permission >> rail.Label('No') >> is_user_already_disabled_41

            can_update_user_end_date_25 >> rail.Label('No') >> trigger_process_timeoff_policies_no_accrual_29
            can_update_user_end_date_25 >> rail.Label('Yes') >> update_user_end_date_26 >> trigger_process_timeoff_policies_no_accrual_29
            trigger_process_timeoff_policies_no_accrual_29 >> wait_for_process_timeoff_policies_29 >> log_disabled_user_30 >> should_trigger_delete_time_and_timeoff

            is_division_gsap >> rail.Label('No') >> disable_user_login_33 >> can_update_user_end_date_34
            can_update_user_end_date_34 >> rail.Label("No") >> trigger_process_timeoff_policies_no_accrual_38
            can_update_user_end_date_34 >> rail.Label("Yes") >> update_user_end_date_35 >> trigger_process_timeoff_policies_no_accrual_38
            trigger_process_timeoff_policies_no_accrual_38 >> wait_for_process_timeoff_policies_38 >> log_disabled_user_39
            log_disabled_user_39 >> should_trigger_delete_time_and_timeoff

            is_user_rehire >> rail.Label("Yes") >> enable_login >> update_user_start_date_48 >> can_update_user_start_date
            is_user_rehire >> rail.Label("No") >> can_update_user_start_date >> rail.Label('No') >> should_disable_user
            can_update_user_start_date >> rail.Label("Yes") >> update_user_start_date_51 >> should_disable_user

            should_disable_user >> rail.Label('Yes') >> update_user_end_date_53 >> is_end_date_less_than_today

            is_end_date_less_than_today >> rail.Label('No') >> no_task_is_end_date_less_than_today >> current_assigned_udf_values
            is_end_date_less_than_today >> rail.Label('Yes') >> disable_user_login_55 >> trigger_process_timeoff_policies_no_accrual_58
            trigger_process_timeoff_policies_no_accrual_58 >> wait_for_process_timeoff_policies_58 >> log_disabled_user_59 >> should_trigger_delete_time_and_timeoff
            should_disable_user >> rail.Label('No') >> current_assigned_udf_values

            current_assigned_udf_values >> get_time_entry_approval_path_name >> get_user_assigned_policy  >> prepare_update_payload >> can_process_user >> rail.Label("Yes") >> update_user >> can_update_timesheet_template\
                >> rail.Label("No") >> update_notification_preference
            can_update_timesheet_template >> rail.Label("Yes") >> update_timesheet_template_with_effective_date >> update_notification_preference
            can_process_user >> rail.Label("No") >> log_ia_exception >> should_trigger_delete_time_and_timeoff

            update_notification_preference >> dummy_process_supervisor

            dummy_process_supervisor >> start_supervisor_update
            end_supervisor_update >> get_assigned_timeoff_types >> get_timeoff_data_from_mapper >> has_any_data
            has_any_data >> rail.Label("Yes") >> get_all_timeoffs >> is_user_rehire_timeoff >> rail.Label("Yes") >> trigger_rehire_timeoff_assignment >> wait_for_trigger_rehire_timeoff_assignment
            has_any_data >> rail.Label("No") >> log_user_completion
            wait_for_trigger_rehire_timeoff_assignment >> get_required_timeoff_type_details
            is_user_rehire_timeoff >> rail.Label("No") >> get_required_timeoff_type_details >> has_any_timeoff_to_assign_or_disable
            has_any_timeoff_to_assign_or_disable >> rail.Label("yes") >> assign_timeoff_to_user >> has_any_timeoff_to_disable >> rail.Label("Yes") >> process_timeoff_no_accrual
            has_any_timeoff_to_assign_or_disable >> rail.Label("No") >> log_user_completion
            process_timeoff_no_accrual >> wait_for_process_timeoff_no_accrual >> has_any_timeoff_types_to_assign
            has_any_timeoff_to_disable >> rail.Label("No") >> has_any_timeoff_types_to_assign
            has_any_timeoff_types_to_assign >> rail.Label("Yes") >> process_timeoffs >> wait_for_process_timeoffs >> reassign_timeoff_to_user >> log_user_completion
            has_any_timeoff_types_to_assign >> rail.Label("No") >> log_user_completion

            log_user_completion >> should_trigger_delete_time_and_timeoff >> rail.Label("Yes") >> trigger_cleanup_for_disabled_user >> wait_for_trigger_cleanup_for_disabled_user >> catch_and_log_error
            should_trigger_delete_time_and_timeoff >> rail.Label("No") >> catch_and_log_error

            _dags.append(dag)
    return _dags

rail.for_each_instance(create_update_user_dag)
