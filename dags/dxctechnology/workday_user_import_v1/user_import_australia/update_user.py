from datetime import timedelta
import json
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_australia.utils import request_payload
from dxctechnology.workday_user_import_v1.user_import.common_utils.response_filter import get_effective_grp_membership_data_handler
from dxctechnology.workday_user_import_v1.user_import_global.utils import custom_methods as gbl_custom_methods
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import should_trigger_delete_time_and_timeoff_for_disabled_user  

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_australia_users_update_user_child_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.user_process_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_australia, default_var='true').lower() == 'true',
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
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users" : [
                    {
                        "uri": "{{dag_run.conf.user_uri}}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else {}
        )

        get_effective_group_membership = rail.RepliconServiceOperator(
            task_id = "get_effective_group_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "dateRange": null
            },
            data_handler=get_effective_grp_membership_data_handler
        )

        get_time_entry_approval_path = rail.RepliconServiceOperator(
            task_id = "get_time_entry_approval_path",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/GetApprovalPathForUser",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        get_user_time_off_type_policy_summary = rail.RepliconServiceOperator(
            task_id = "get_user_time_off_type_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        get_custom_fields_to_update = rail.PythonOperator(
            task_id = "get_custom_fields_to_update",
            python_callable=request_payload.get_custom_fields_to_update_callable
        )

        can_process_user = rail.IfOperator(
            task_id = "can_process_user",
            test=lambda: not bool(rail.result("get_custom_fields_to_update", "ia_exception_msg")),
            yes_task="has_any_udfs_to_update",
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
                "Details": rail.result("get_custom_fields_to_update", "ia_exception_msg")
            }
        )


        has_any_udfs_to_update = rail.IfOperator(
            task_id = "has_any_udfs_to_update",
            test=lambda: bool(rail.result("get_custom_fields_to_update")),
            yes_task="update_user_udfs",
            no_task="can_update_user_permission"
        )

        update_user_udfs = rail.RepliconServiceOperator(
            task_id = "update_user_udfs",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['user_uri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "customFieldValuesToApply": rail.result("get_custom_fields_to_update"),
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )


        def can_update_user_permission_test(dag_run):
            permission_change = rail.result('get_custom_fields_to_update','permission_change')
            if dag_run.conf['connect_employee'] == 'Yes' and permission_change['update_permission_connect'] is True:
                return True
            if dag_run.conf['connect_employee'] == 'No' and permission_change['update_permission_general'] is True:
                return True
            return False

        can_update_user_permission = rail.IfOperator(
            task_id = "can_update_user_permission",
            test=can_update_user_permission_test,
            yes_task="update_user_permission",
            no_task="get_assigned_permission_for_user"
        )

        def update_user_permission_payload(dag_run):
            permission_to_add = dag_run.conf['user_permission_sets']['connect_employee']
            if dag_run.conf['connect_employee'] == 'No' and rail.result('get_custom_fields_to_update','permission_change')['update_permission_general'] is True:
                permission_to_add = dag_run.conf['user_permission_sets']['end_user_permission']
            return {
                "userUri" : dag_run.conf['user_uri'],
                "permissionSetUri": permission_to_add
            }

        update_user_permission = rail.RepliconServiceOperator(
            task_id="update_user_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=update_user_permission_payload
        )

        get_assigned_permission_for_user = rail.RepliconServiceOperator(
            task_id="get_assigned_permission_for_user",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                'userUri': "{{dag_run.conf.user_uri}}"
            }
        )

        is_user_disabled_for_non_go_live_country = rail.IfOperator(
            task_id = "is_user_disabled_for_non_go_live_country",
            test = lambda dag_run: gbl_custom_methods.is_user_disabled_for_non_go_live_country_test(dag_run, get_user_details.task_id),
            yes_task = "has_no_project_management_permission",
            no_task = "empty_is_user_disabled_and_replicon_field_false"
        )

        empty_is_user_disabled_and_replicon_field_false = rail.EmptyOperator(
            task_id = "empty_is_user_disabled_and_replicon_field_false"
        )
     
        has_no_project_management_permission = rail.IfOperator(
            task_id = "has_no_project_management_permission",
            test=gbl_custom_methods.user_has_no_project_management_permission_test,
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
            test=gbl_custom_methods.is_division_gsap_test,
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
            current_timeoff_policies = rail.result("get_user_time_off_type_policy_summary")['policiesByTimeOffType']

            return list(filter(lambda to_policy:  to_policy['isTimeOffAllowedAgainstThisTimeOffType'] is True
                                and bool(to_policy['policySetSchedule'] and to_policy['policySetSchedule'][0]['effectiveDate'].get('day')), current_timeoff_policies))

        trigger_process_timeoff_policies = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_process_timeoff_policies",
            trigger_dag_id=config.workday_user_import_global_users_update_user_timeoff_process_child_dag_disable,
            items=get_trigger_process_timeoff_policies_items,
            conf=lambda item, dag_run: {
                "user_log": dag_run.conf['user_log'],
                "file_name": dag_run.conf["master_file_name"],
                "user_uri": dag_run.conf["user_uri"],
                "timeoff_uri": item['timeOffType']["uri"],
                "file_data": {
                    "email_id": dag_run.conf['file_data']['email_id'],
                    "emp_id" : dag_run.conf['file_data']['emp_id']
                },
                "policy_set": json.dumps(item["policySetSchedule"]).replace("[[{", "[{").replace("}]]", "}]"),
                "end_date": dag_run.conf['file_data']['term_date'],
                "end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "prevent_balance_overdraw_uri": dag_run.conf["prevent_balance_overdraw_uri"],
                "parent_location" : rail.result("get_effective_group_membership")['parent_location'].get("location", {}).get('displayText', ''),
                "is_ia_updated": rail.result("prepare_update_user_payload", "ia_updated"),
                "ia_end_date": dag_run.conf['json_formatted_dates']['ia_end_date'],
                "ia_start_date": dag_run.conf['json_formatted_dates']['ia_start_date'],
                "assignment_type": dag_run.conf['file_data']['assignment_type'],
                "ia_updated": rail.result("prepare_update_user_payload", "ia_updated"),
                "is_ia": dag_run.conf['file_data']['is_ia'],
                "assignment_type": dag_run.conf['file_data']['assignment_type'],
                "ia_start_date": dag_run.conf['file_data']['ia_start_date'],
                "ia_end_date": dag_run.conf['file_data']['ia_end_date'],
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
            properties=lambda dag_run: gbl_custom_methods.get_disable_user_log_message(
                dag_run
            )
        )

        user_does_not_have_admin_and_payroll_permission = rail.IfOperator(
            task_id= "user_does_not_have_admin_and_payroll_permission",
            test=gbl_custom_methods.user_does_not_have_admin_and_payroll_permission_test,
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
            test=gbl_custom_methods.can_update_user_end_date_test,
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
            test= gbl_custom_methods.is_user_disabled_and_replicon_field_false_test,
            yes_task="dummy_is_user_disabled_and_replicon_field_false_yes_task",
            no_task="is_user_for_disable"
        )

        dummy_is_user_disabled_and_replicon_field_false_yes_task= rail.EmptyOperator(
            task_id = "dummy_is_user_disabled_and_replicon_field_false_yes_task"
        )

        can_update_user_end_date_2 = rail.IfOperator(
            task_id = "can_update_user_end_date_2",
            test=gbl_custom_methods.can_update_user_end_date_test,
            yes_task= "update_user_end_date_3",
            no_task="log_disabled_user_106"
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
        
        log_disabled_user_106 =  rail.WriteLogOperator(
            task_id = "log_disabled_user_106",
            log = "{{dag_run.conf.user_log}}",
            message="User Disable",
            severity="Skipped",
            properties=gbl_custom_methods.get_error_message_for_long_leave_or_user_disabled_with_replicon_field_false
        )


        def is_user_for_disable_test(dag_run):          
            user_details = rail.result('get_user_details')
            return user_details['userDetails']['isEnabled'] is True \
                and dag_run.conf['replicon_field'] in ['false', False]

        is_user_for_disable = rail.IfOperator(
            task_id = "is_user_for_disable",
            test=is_user_for_disable_test,
            yes_task="update_user_start_end_date",
            no_task="is_already_disable_for_long_leave"
        )

        update_user_start_end_date = rail.RepliconServiceOperator(
            task_id = "update_user_start_end_date",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": dag_run.conf['json_formatted_dates']['term_date'],
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        is_term_date_less_than_today = rail.IfOperator(
            task_id = "is_term_date_less_than_today",
            test=gbl_custom_methods.is_end_date_less_than_today_test,
            yes_task="disable_login",
            no_task="is_already_disable_for_long_leave"
        )

        disable_login = rail.RepliconServiceOperator(
            task_id = "disable_login",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri']
            }
        )

        def is_already_disable_for_long_leave_test(dag_run):          
            user_details = rail.result('get_user_details')
            return user_details['userDetails']['isEnabled'] is not True \
                and dag_run.conf['file_data']['on_leave'] in ['1', 1]\
                and gbl_custom_methods.user_has_no_project_management_permission_test()

        is_already_disable_for_long_leave = rail.IfOperator(
            task_id = "is_already_disable_for_long_leave",
            test=is_already_disable_for_long_leave_test,
            yes_task="log_disable_for_long_leave",
            no_task="is_for_disable_long_leave"
        )

        log_disable_for_long_leave = rail.WriteLogOperator(
            task_id = "log_disable_for_long_leave",
            log = "{{dag_run.conf.user_log}}",
            message="User Update",
            severity="Skipped",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf['file_data']["emp_id"],
                "Email": dag_run.conf['file_data']["email_id"],
                "Action": 'Update',
                "Status": "Skipped",
                "Details": 'User already disabled in Replicon for "On Leave" is set to 1 in feed file'
            }
        )

        def is_for_disable_long_leave_test(dag_run):          
            user_details = rail.result('get_user_details')
            return user_details['userDetails']['isEnabled'] is True \
                and dag_run.conf['file_data']['on_leave'] in ['1', 1]\
                and gbl_custom_methods.user_has_no_project_management_permission_test()

        is_for_disable_long_leave = rail.IfOperator(
            task_id = "is_for_disable_long_leave",
            test=is_for_disable_long_leave_test,
            yes_task="disable_login2",
            no_task="is_on_leave_rehire_or_rehire"
        )

        disable_login2 = rail.RepliconServiceOperator(
            task_id="disable_login2",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri']
            }
        )

        def can_update_end_date_test(dag_run):
            user_details = rail.result('get_user_details')
            return bool(bool(not user_details['userDetails']['employmentDateRange']['endDate']) and bool(dag_run.conf['json_formatted_dates']['term_date']))

        can_update_end_date = rail.IfOperator(
            task_id = "can_update_end_date",
            test=can_update_end_date_test,
            yes_task="update_user_start_end_date_2",
            no_task="is_termination_date_present_2"
        )

        update_user_start_end_date_2 = rail.RepliconServiceOperator(
            task_id = "update_user_start_end_date_2",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": dag_run.conf['json_formatted_dates']['term_date'],
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        is_termination_date_present_2 = rail.IfOperator(
            task_id = "is_termination_date_present_2",
            test="{{dag_run.conf.file_data.term_date | is_truthy}}",
            yes_task="trigger_process_timeoff_policies",
            no_task="log_disabled_user_22"
        )

        def is_on_leave_rehire_or_rehire_test(dag_run):
            user_details = rail.result('get_user_details')
            is_on_leave_rehire = user_details['userDetails']['isEnabled'] is not True \
                and dag_run.conf['file_data']['on_leave'] in ['0', 0]\
                and rail.result('get_custom_fields_to_update', 'on_leave_status_update')
            rehire = user_details['userDetails']['isEnabled'] is not True \
                and dag_run.conf['replicon_field'] in ['true', 'True', True] \
                and dag_run.conf['mapper_data']['profile_status'].lower() == 'enabled' \
                and (not rail.result('get_custom_fields_to_update', 'on_leave_status_update'))\
                and dag_run.conf['file_data']['on_leave'] in ['0', 0]
            if rehire:
                rail.set_result(key='rehire', val='Yes')
            return is_on_leave_rehire or rehire

        is_on_leave_rehire_or_rehire = rail.IfOperator(
            task_id = "is_on_leave_rehire_or_rehire",
            test=is_on_leave_rehire_or_rehire_test,
            yes_task="enabled_login",
            no_task="process_update_user"
        )

        enabled_login = rail.RepliconServiceOperator(
            task_id = "enabled_login",
            endpoint = "/services/SecurityService1.svc/EnableLogin",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
            }
        )

        def can_update_term_exported_aus_test():
            term_exported_aus = rail.find_first_by_attr_and_get_attr(rail.result("get_user_details")['userDetails']['customFieldValues'],
            'customField.displayText', 'Term Exported (AUS)', 'text')
            return term_exported_aus == 'Yes'

        can_update_term_exported_aus = rail.IfOperator(
            task_id = "can_update_term_exported_aus",
            test = can_update_term_exported_aus_test,
            yes_task="update_term_exported_aus_to_no",
            no_task="update_user_start_date"
        )

        update_term_exported_aus_to_no = rail.RepliconServiceOperator(
            task_id = "update_term_exported_aus_to_no",
            endpoint="services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result("get_user_details")['userDetails']['customFieldValues'],
            'customField.displayText', 'Term Exported (AUS)', 'customField', {}).get('uri'),
                "value": "No"
            }
        )

        update_user_start_date = rail.RepliconServiceOperator(
            task_id = "update_user_start_date",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        process_update_user = rail.EmptyOperator(
            task_id = "process_update_user"
        )

        get_timeentry_approval_path_for_user = rail.RepliconServiceOperator(
            task_id="get_timeentry_approval_path_for_user",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/GetApprovalPathForUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        get_assigned_policy_sets_for_user = rail.RepliconServiceOperator(
            task_id="get_assigned_policy_sets_for_user",
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        update_user = rail.RepliconServiceOperator(
            task_id = "update_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.update_user_payload(dag_run, config)
        )

        def get_timeoff_data_callable(dag_run):
            country = dag_run.conf['file_data']['country']
            ia = dag_run.conf['file_data']['is_ia']
            uri = dag_run.conf['file_data']['ausjc'] if dag_run.conf['file_data']['ausjc'] else dag_run.conf['file_data']['industrial_instrument_classification']

            time_off_data = list(filter(lambda row:  row['Type'] == "Timeoff" and
                                row['Function'] == "Workday User Sync" and
                                row['Country'] == country  and
                                row['URI'] == uri and
                                row['personnelsubarea'] == ia ,config.MAPPER))
            rail.set_result(key="has_data", val=bool(time_off_data))
            return time_off_data


        get_timeoff_data = rail.PythonOperator(
            task_id = "get_timeoff_data",
            python_callable=get_timeoff_data_callable
        )

        has_any_timeoffs_to_process = rail.IfOperator(
            task_id = "has_any_timeoffs_to_process",
            test= lambda: rail.result("get_timeoff_data", 'has_data') is True,
            yes_task="process_timeoff_assignment",
            no_task="add_for_supervisor_assignment"
        )

        process_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_timeoff_assignment",
            trigger_dag_id=config.workday_user_import_australia_users_update_user_timeoff_process_child_dag,
            items=[1],
            conf=lambda dag_run:{
                **{
                    "file_name": dag_run.conf["master_file_name"],
                    "user_uri": dag_run.conf["user_uri"],
                    "user_log": dag_run.conf['user_log'],
                    "emp_id": dag_run.conf['file_data']["emp_id"],
                    "email_id": dag_run.conf['file_data']["email_id"],
                    "loginName": rail.result('update_user')['loginName'],
                    "start_date": {},
                    "hire_date": dag_run.conf['file_data']['hire_date'],
                    "state": dag_run.conf['file_data']['state'],
                    "end_date": dag_run.conf['file_data']['term_date'],
                    "end_date_json": dag_run.conf['json_formatted_dates']['term_date'],
                    "company_code": dag_run.conf['file_data']['company_code'],
                    "parent_company_code": dag_run.conf['file_data']['parent_company'],
                    "country": dag_run.conf['file_data']['country'],
                    "timeoffs": rail.result("get_timeoff_data"),
                    "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                    "prevent_balance_overdraw_uri": dag_run.conf["prevent_balance_overdraw_uri"],
                    "parent_location" : rail.result("get_effective_group_membership")['parent_location'],
                    "rehire": rail.result("is_on_leave_rehire_or_rehire", "rehire"),
                    "ausjc": dag_run.conf['file_data']['ausjc'],
                    "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0,
                    "fte_updated": rail.result("update_user", "fte_updated"),
                    "location_updated": rail.result("update_user", "location_updated"),
                    "locationeffectivedate": dag_run.conf['json_formatted_dates']['location_effective_date'],
                    "is_ia_updated": rail.result("get_custom_fields_to_update", "ia_updated"),
                    "ia_updated": rail.result("get_custom_fields_to_update", "ia_updated"),
                    "is_ia": dag_run.conf['file_data']['is_ia'],
                    "ia_end_date": dag_run.conf['file_data']['ia_end_date'],
                    "ia_start_date": dag_run.conf['file_data']['ia_start_date'],
                },
                **dag_run.conf
            },
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        wait_for_process_timeoff_assignment = rail.WaitForDagRunsSensor(
            task_id = "wait_for_process_timeoff_assignment",
            dag_runs="{{result('process_timeoff_assignment')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
        )

        add_for_supervisor_assignment = rail.WriteLogOperator(
            task_id = "add_for_supervisor_assignment",
            message = "User Update (Aus) | Supervisor assignment",
            log="{{dag_run.conf.supervisor_user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'], #Emplid
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Update",
                "Status": "pending",
                "Details": "Supervisor Reassignment",
                "state": dag_run.conf['file_data']['state'],
                "login_name": dag_run.conf['file_data']['email_id'],
                "user_uri|country": f"{dag_run.conf['user_uri']}|{dag_run.conf['file_data']['country']}",
                "user_name": f"{dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['last_name']}",
                "supervisor_login_name": f"{dag_run.conf['file_data']['supervisor_email_id']}|{dag_run.conf['file_data']['supervisor_id']}|{dag_run.conf['file_data']['supervisor_f_name']}|{dag_run.conf['file_data']['supervisor_l_name']}",
                "effective_date": dag_run.conf['json_formatted_dates']['supervisor_date'],
                "user_log": dag_run.conf['user_log'],
                "supervisor_end_user_permission": dag_run.conf['user_permission_sets']['supervisor_end_user_permission'],
                "supervisor_user_permission": dag_run.conf['user_permission_sets']['supervisor_user_permission'],
                'aus_supervisor_end_user_permission': dag_run.conf['user_permission_sets']['aus_supervisor_end_user_permission'],
                'parent_company': dag_run.conf['file_data']['parent_company'],
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

        get_user_details >> get_effective_group_membership >> get_time_entry_approval_path >> get_user_time_off_type_policy_summary >> get_custom_fields_to_update
        get_custom_fields_to_update >> can_process_user >> rail.Label("Yes") >> has_any_udfs_to_update >> rail.Label("Yes") >> update_user_udfs >> can_update_user_permission
        has_any_udfs_to_update >> rail.Label("No") >> can_update_user_permission

        can_process_user >> rail.Label("No") >> log_ia_exception >> should_trigger_delete_time_and_timeoff

        can_update_user_permission >> rail.Label("Yes") >> update_user_permission >> get_assigned_permission_for_user
        can_update_user_permission >> rail.Label("No") >> get_assigned_permission_for_user >> is_user_disabled_for_non_go_live_country

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
            >> update_user_end_date_3 >> log_disabled_user_106 >> should_trigger_delete_time_and_timeoff
        can_update_user_end_date_2 >> rail.Label("No") >> log_disabled_user_106


        is_user_disabled_for_non_go_live_country >> rail.Label("No") >> empty_is_user_disabled_and_replicon_field_false >> is_user_disabled_and_replicon_field_false

        is_user_disabled_and_replicon_field_false >> rail.Label("No") >> is_user_for_disable >> rail.Label("Yes") >> update_user_start_end_date
        update_user_start_end_date >> is_term_date_less_than_today >> rail.Label("Yes") >> disable_login >> trigger_process_timeoff_policies
        is_term_date_less_than_today >> rail.Label("No") >> is_already_disable_for_long_leave
        is_user_for_disable >> rail.Label("No") >> is_already_disable_for_long_leave >> rail.Label("Yes") >> log_disable_for_long_leave >> should_trigger_delete_time_and_timeoff

        is_already_disable_for_long_leave >> rail.Label("No") >> is_for_disable_long_leave >> rail.Label(
            "Yes")>> disable_login2 >> can_update_end_date
        is_for_disable_long_leave >> rail.Label("No") >> is_on_leave_rehire_or_rehire
        can_update_end_date >> rail.Label("Yes") >> update_user_start_end_date_2 >> is_termination_date_present_2
        can_update_end_date >> rail.Label("No") >> is_termination_date_present_2
        is_termination_date_present_2 >> rail.Label("Yes") >> trigger_process_timeoff_policies
        is_termination_date_present_2 >> rail.Label("No") >> log_disabled_user_22

        is_on_leave_rehire_or_rehire >> rail.Label("Yes") >> enabled_login >> can_update_term_exported_aus
        can_update_term_exported_aus >> rail.Label("Yes") >> update_term_exported_aus_to_no >> update_user_start_date
        can_update_term_exported_aus >> rail.Label("No") >> update_user_start_date >> process_update_user
        is_on_leave_rehire_or_rehire >> rail.Label("No") >> process_update_user >> \
            get_timeentry_approval_path_for_user >> get_assigned_policy_sets_for_user >> update_user >> get_timeoff_data >> has_any_timeoffs_to_process
        has_any_timeoffs_to_process >> rail.Label("No") >> add_for_supervisor_assignment >> should_trigger_delete_time_and_timeoff
        has_any_timeoffs_to_process >> rail.Label("Yes") >> process_timeoff_assignment >> wait_for_process_timeoff_assignment >> add_for_supervisor_assignment >> should_trigger_delete_time_and_timeoff
        should_trigger_delete_time_and_timeoff >> rail.Label("Yes") >> trigger_cleanup_for_disabled_user >> wait_for_trigger_cleanup_for_disabled_user >> catch_and_log_error
        should_trigger_delete_time_and_timeoff >> rail.Label("No") >> catch_and_log_error

    return dag

rail.for_each_instance(create_dag)
