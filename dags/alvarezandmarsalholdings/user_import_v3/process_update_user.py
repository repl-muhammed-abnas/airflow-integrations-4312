from datetime import datetime, timedelta
from pendulum import now
from airflow.models import Variable
import rail
import json
from alvarezandmarsalholdings.user_import_v3.utils import request_payload, custom_methods


null = None

# pylint: disable=too-many-statements


def create_child_dag(config):
    add_dags = []

    for idx in range(0, config.BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.process_update_users_dagid}{get_postfix}",
            description='Alvarezandmarsalholdings - User Import - Process Update Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_new_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='get_user_details'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='get_user_details',
                end_task='catch_and_log_errors',
            )

            get_user_details = rail.RepliconServiceOperator(
                task_id="get_user_details",
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=lambda dag_run: {
                    "users": [
                        {
                            "uri": null,
                            "loginName": null,
                            "employeeId": dag_run.conf["employee_id"],
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
                yes_task='get_existing_worker_subtype',
                no_task='log_user_not_present_in_replicon'

            )

            get_existing_worker_subtype = rail.RepliconServiceOperator(
                task_id='get_existing_worker_subtype',
                endpoint="/services/EmployeeTypeService1.svc/GetEmployeeTypeForUser",
                data=lambda: {
                    "userUri": rail.result('get_user_details')['userDetails']['uri']
                }
            )

            if_worker_subtype_is_regular = rail.IfOperator(
                task_id='if_worker_subtype_is_regular',
                test=lambda dag_run: dag_run.conf['employee_type'] in [
                    'Regular', 'Fixed Term', 'Intern'],
                yes_task='get_effective_user_group_membership',
                no_task='if_worker_subtype_is_subcontractor'
            )

            log_user_not_present_in_replicon = rail.WriteLogOperator(
                task_id='log_user_not_present_in_replicon',
                log='{{ dag_run.conf.user_log }}',
                message="User not present in Replicon",
                severity='Exception',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    "action": "Update",
                    "status": "Exception",
                    'details': "User not present in replicon"
                }
            )

            if_worker_subtype_is_subcontractor = rail.IfOperator(
                task_id='if_worker_subtype_is_subcontractor',
                test=lambda dag_run: dag_run.conf['employee_type'] == 'Subcontractor',
                yes_task='if_subcontractor_user_is_enabled',
                no_task='if_new_is_agency_temp_from_subcontractor'
            )

            if_new_is_agency_temp_from_subcontractor = rail.IfOperator(
                task_id='if_new_is_agency_temp_from_subcontractor',
                test=lambda dag_run: dag_run.conf['employee_type'] == 'Agency Temp' and (
                    rail.result('get_existing_worker_subtype') or {}).get('displayText') == 'Subcontractor',
                yes_task='if_email_domain_matched',
                no_task='if_worker_subtype_is_other'
            )

            if_subcontractor_user_is_enabled = rail.IfOperator(
                task_id='if_subcontractor_user_is_enabled',
                test=lambda: str(rail.result('get_user_details')['userDetails']['isEnabled']).lower() == 'true',
                yes_task='compute_subcontractor_end_date',
                no_task='update_disabled_subcontractor_end_date'
            )

            compute_subcontractor_end_date = rail.PythonOperator(
                task_id='compute_subcontractor_end_date',
                python_callable=lambda dag_run: request_payload.get_subcontractor_end_date(dag_run)
            )

            update_subcontractor_user = rail.RepliconServiceOperator(
                task_id='update_subcontractor_user',
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=lambda dag_run: request_payload.get_subcontractor_disable_payload(dag_run)
            )

            log_subcontractor_disabled_exception = rail.WriteLogOperator(
                task_id='log_subcontractor_disabled_exception',
                log='{{ dag_run.conf.user_log }}',
                message="Worker Sub Type updated to Subcontractor and user disabled with an end date",
                severity='Success',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    "action": "Update",
                    "status": "Success",
                    'details': "Worker Sub Type updated to Subcontractor and user disabled with an end date"
                }
            )

            update_disabled_subcontractor_end_date = rail.RepliconServiceOperator(
                task_id='update_disabled_subcontractor_end_date',
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=lambda dag_run: request_payload.get_disabled_subcontractor_end_date_payload(dag_run)
            )

            log_subcontractor_already_disabled_exception = rail.WriteLogOperator(
                task_id='log_subcontractor_already_disabled_exception',
                log='{{ dag_run.conf.user_log }}',
                message=lambda dag_run: "User end date updated, user is already disabled with Worker Sub Type Subcontractor"
                    if dag_run.conf.get("end_date")
                    else "User is already disabled with Worker Sub Type Subcontractor, no end date provided",
                severity='Success',
                properties=lambda dag_run: {
                    'employee_id': dag_run.conf["employee_id"],
                    "action": "Update",
                    "status": "Success",
                    'details': "User end date updated, user is already disabled with Worker Sub Type Subcontractor"
                        if dag_run.conf.get("end_date")
                        else "User is already disabled with Worker Sub Type Subcontractor, no end date provided"
                }
            )

            if_worker_subtype_is_other = rail.IfOperator(
                task_id='if_worker_subtype_is_other',
                test=lambda dag_run: dag_run.conf['employee_type'] in ['Agency Temp'],
                yes_task='if_email_domain_matched',
                no_task='log_user_not_updated_worker_subtype_different'
            )

            if_email_domain_matched = rail.IfOperator(
                task_id='if_email_domain_matched',
                test=lambda dag_run: dag_run.conf['email'].split(
                    '@')[1] == 'alvarezandmarsal.com',
                yes_task='get_effective_user_group_membership',
                no_task='log_user_not_updated_worker_subtype_email_domain_different'
            )

            get_effective_user_group_membership = rail.RepliconServiceOperator(
                task_id='get_effective_user_group_membership',
                endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
                data=lambda dag_run: {
                    "userUri": rail.result("get_user_details")["userDetails"]['uri'],
                    "dateRange": null
                }
            )

            get_placeholder_policyset = rail.RepliconServiceOperator(
                task_id='get_placeholder_policyset',
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
                data={
                    "timeOffTypeUri": "{{ dag_run.conf.placeholder_timeoffuri }}"
                },
                data_handler=lambda res: json.loads(json.dumps(res[0]['policySet'], ensure_ascii=False).replace('"null"', '"effective"').replace(
                    '"script"', '"scriptTarget"'))
            )

            get_applicable_timeoff_types = rail.PythonOperator(
                task_id='get_applicable_timeoff_types',
                python_callable=lambda dag_run: request_payload.get_time_off_types(
                    config, dag_run),
            )

            def get_applicable_timeoffs_name_uri_lists(res, applicable_timeoff_types):
                fte_100_timeoffs_to_assign_list = []
                fte_any_timeoffs_to_assign_list = []

                for timeoff_name in applicable_timeoff_types['timeoff_for_100_fte']:
                    fte_100_timeoffs_to_assign_list.append({
                        "timeoff_name": timeoff_name,
                        "uri": rail.find_first_by_attr_and_get_attr(res, 'displayText', timeoff_name, 'uri')
                    })

                for timeoff_name in applicable_timeoff_types['timeoff_for_any_fte']:
                    fte_any_timeoffs_to_assign_list.append({
                        "timeoff_name": timeoff_name,
                        "uri": rail.find_first_by_attr_and_get_attr(res, 'displayText', timeoff_name, 'uri')
                    })

                return {
                    "fte_100_timeoffs_to_assign_list": fte_100_timeoffs_to_assign_list,
                    "fte_100_timeoffs_to_assign_uris": [timeoff["uri"] for timeoff in fte_100_timeoffs_to_assign_list if timeoff["uri"]],
                    "fte_any_timeoffs_to_assign_list": fte_any_timeoffs_to_assign_list,
                    "fte_any_timeoffs_to_assign_uris": [timeoff["uri"] for timeoff in fte_any_timeoffs_to_assign_list if timeoff["uri"]]
                }

            get_all_applicable_timeoff_types = rail.RepliconServiceOperator(
                task_id='get_all_applicable_timeoff_types',
                endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
                data_handler=lambda res: get_applicable_timeoffs_name_uri_lists(
                    res, rail.result('get_applicable_timeoff_types'))
            )

            log_existing_timeoff_policies_for_user = rail.RepliconServiceOperator(
                task_id='log_existing_timeoff_policies_for_user',
                endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
                data={
                    "userUri": "{{ dag_run.conf.useruri }}"  # check this
                },
                data_handler=lambda res: res["policiesByTimeOffType"] if res["policiesByTimeOffType"] else [
                ]
            )

            list_uri_for_existing_timeoff_policies = rail.PythonOperator(
                task_id='list_uri_for_existing_timeoff_policies',
                python_callable=lambda: custom_methods.get_list_of_uri_for_existing_timeoff_policies()
            )

            final_timeoff_policy_payload_variable = rail.SetVariableOperator(
                task_id='final_timeoff_policy_payload_variable',
                name='final_timeoff_policy_payload',
                value=[]
            )

            if_fte_100_timeoffs_to_assign = rail.IfOperator(
                task_id='if_fte_100_timeoffs_to_assign',
                test=lambda: bool(rail.result('get_all_applicable_timeoff_types')[
                                  'fte_100_timeoffs_to_assign_uris']),
                yes_task='foreach_applicable_timeoff_types_fte_100',
                no_task='if_fte_any_timeoffs_to_assign'
            )

            foreach_applicable_timeoff_types_fte_100 = rail.ForEachOperator(
                task_id='foreach_applicable_timeoff_types_fte_100',
                items=lambda: rail.result('get_all_applicable_timeoff_types')[
                    'fte_100_timeoffs_to_assign_uris'],
                start_task='default_timeoff_policyset_for_timeoff_type',
                end_task='foreach_applicable_timeoff_types_fte_100_end',
            )

            default_timeoff_policyset_for_timeoff_type = rail.RepliconServiceOperator(
                task_id='default_timeoff_policyset_for_timeoff_type',
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
                data={
                    "timeOffTypeUri": "{{ result('foreach_applicable_timeoff_types_fte_100') }}"
                },
                data_handler=lambda res: res[0]['policySet'] if res else []
            )

            existing_policysetschedule_if_timeoff_policy_is_existing_in_user = rail.PythonOperator(
                task_id='existing_policysetschedule_if_timeoff_policy_is_existing_in_user',
                python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                    'log_existing_timeoff_policies_for_user'), 'timeOffType.uri', rail.result('foreach_applicable_timeoff_types_fte_100'), "policySetSchedule", [])
            )

            log_relevant_historical_policies = rail.PythonOperator(
                task_id='log_relevant_historical_policies',
                python_callable=lambda dag_run: custom_methods.get_relevant_historical_policies(rail.result(
                    'existing_policysetschedule_if_timeoff_policy_is_existing_in_user'), dag_run.conf["run_date"]),
            )

            final_policyset_schedule_for_timeoff_type = rail.PythonOperator(
                task_id='final_policyset_schedule_for_timeoff_type',
                python_callable=lambda dag_run: custom_methods.get_final_policyset_schedule_for_timeoff_type(dag_run, rail.result('foreach_applicable_timeoff_types_fte_100'),
                    rail.result('log_relevant_historical_policies'), rail.result('default_timeoff_policyset_for_timeoff_type'), dag_run.conf["run_date"]),
            )

            add_policy_to_final_timeoff_policy_payload_variable = rail.SetVariableOperator(
                task_id='add_policy_to_final_timeoff_policy_payload_variable',
                name='final_timeoff_policy_payload',
                append=True,
                value=lambda: {
                    "timeOffType": {
                        "uri": rail.result('foreach_applicable_timeoff_types_fte_100')
                    },
                    "isTimeOffAllowedAgainstThisTimeOffType": True,
                    "applyDefaultTimeOffTypePolicy": False,
                    "defaultTimeOffTypePolicyEffectiveDate": None,
                    "policySchedule": rail.result('final_policyset_schedule_for_timeoff_type')
                }
            )

            foreach_applicable_timeoff_types_fte_100_end = rail.EmptyOperator(
                task_id='foreach_applicable_timeoff_types_fte_100_end'
            )

            if_fte_any_timeoffs_to_assign = rail.IfOperator(
                task_id='if_fte_any_timeoffs_to_assign',
                test=lambda: bool(rail.result('get_all_applicable_timeoff_types')[
                                  'fte_any_timeoffs_to_assign_uris']),
                yes_task='foreach_applicable_timeoff_types_fte_any',
                no_task='update_user'
            )

            foreach_applicable_timeoff_types_fte_any = rail.ForEachOperator(
                task_id='foreach_applicable_timeoff_types_fte_any',
                items=lambda: rail.result('get_all_applicable_timeoff_types')[
                    'fte_any_timeoffs_to_assign_uris'],
                start_task='existing_policysetschedule_fte_any_if_timeoff_policy_is_existing_for_user',
                end_task='foreach_applicable_timeoff_types_fte_any_end',
            )

            existing_policysetschedule_fte_any_if_timeoff_policy_is_existing_for_user = rail.PythonOperator(
                task_id='existing_policysetschedule_fte_any_if_timeoff_policy_is_existing_for_user',
                python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                    'log_existing_timeoff_policies_for_user'), 'timeOffType.uri', rail.result('foreach_applicable_timeoff_types_fte_any'), "policySetSchedule", [])
            )

            log_relevant_historical_policies_fte_any_timeoff_type = rail.PythonOperator(
                task_id='log_relevant_historical_policies_fte_any_timeoff_type',
                python_callable=lambda dag_run: custom_methods.get_relevant_historical_policies(rail.result(
                    'existing_policysetschedule_fte_any_if_timeoff_policy_is_existing_for_user'), dag_run.conf["run_date"]),
            )

            final_policyset_schedule_for_fte_any_timeoff_type = rail.PythonOperator(
                task_id='final_policyset_schedule_for_fte_any_timeoff_type',
                python_callable=lambda dag_run: custom_methods.get_final_policyset_schedule_for_timeoff_type(dag_run, rail.result('foreach_applicable_timeoff_types_fte_any'),
                    rail.result('log_relevant_historical_policies_fte_any_timeoff_type'), rail.result('get_placeholder_policyset'), dag_run.conf["run_date"]),
            )

            add_fte_any_timeoff_policy_to_final_timeoff_policy_payload_variable = rail.SetVariableOperator(
                task_id='add_fte_any_timeoff_policy_to_final_timeoff_policy_payload_variable',
                name='final_timeoff_policy_payload',
                append=True,
                value=lambda: {
                    "timeOffType": {
                        "uri": rail.result('foreach_applicable_timeoff_types_fte_any')
                    },
                    "isTimeOffAllowedAgainstThisTimeOffType": True,
                    "applyDefaultTimeOffTypePolicy": False,
                    "defaultTimeOffTypePolicyEffectiveDate": None,
                    "policySchedule": rail.result('final_policyset_schedule_for_fte_any_timeoff_type')
                }
            )

            foreach_applicable_timeoff_types_fte_any_end = rail.EmptyOperator(
                task_id='foreach_applicable_timeoff_types_fte_any_end'
            )

            update_user = rail.RepliconServiceOperator(
                task_id="update_user",
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=lambda dag_run: request_payload.get_update_user_payload(
                    dag_run, config, rail.get_dag_run_var('final_timeoff_policy_payload'))
            )

            is_login_name_changed = rail.IfOperator(
                task_id='is_login_name_changed',
                test=lambda dag_run: rail.result('get_user_details')[
                    "securityConfiguration"]["loginName"] != dag_run.conf["workday_user_name"],
                yes_task='update_user_login_name',
                no_task='write_user_sucessfully_updated'
            )

            update_user_login_name = rail.RepliconServiceOperator(
                task_id='update_user_login_name',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data={
                    "user": {
                        "uri": '{{result("get_user_details").userDetails.uri}}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "modifications":  {
                        "securitySettingsToApply": {
                            "loginName": "{{dag_run.conf.workday_user_name}}",
                            "ssoName": "{{dag_run.conf.workday_user_name}}"
                        }
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
            )

            write_user_sucessfully_updated = rail.WriteLogOperator(
                task_id='write_user_sucessfully_updated',
                log='{{ dag_run.conf.user_log }}',
                message="User updated successfully",
                severity='Success',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    "action": "Update",
                    "status": "Success",
                    'details': "User updated successfully"
                }
            )

            log_user_not_updated_worker_subtype_different = rail.WriteLogOperator(
                task_id='log_user_not_updated_worker_subtype_different',
                log='{{ dag_run.conf.user_log }}',
                message="User not Updated, Worker subtype out of scope",
                severity='Exception',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    "action": "Update",
                    "status": "Exception",
                    'details': "User not Updated, Worker subtype out of scope"
                }
            )

            log_user_not_updated_worker_subtype_email_domain_different = rail.WriteLogOperator(
                task_id='log_user_not_updated_worker_subtype_email_domain_different',
                log='{{ dag_run.conf.user_log }}',
                message="User not Updated, email domain mismatch",
                severity='Exception',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    "action": "Update",
                    "status": "Exception",
                    'details': "User not Updated, email domain mismatch"
                }
            )

            get_effective_supervisor_of_user = rail.RepliconServiceOperator(
                task_id="get_effective_supervisor_of_user",
                endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
                data={
                    "userUri": "{{ result('get_user_details')['userDetails']['uri']}}",
                    "asOfDate": custom_methods.get_today_date()
                }
            )

            write_supervisor_pending_logs = rail.WriteLogOperator(
                task_id="write_supervisor_pending_logs",
                log='{{dag_run.conf.supervisor_log}}',
                message="Supervisor",
                severity="Pending",
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf["employee_id"],
                    "reporting_manager": dag_run.conf["reporting_manager"],
                    "reporting_manager_effective_date": dag_run.conf["reporting_manager_effective_date"],
                    "Add_Update": "Update",
                    "type": "reporting_manager",
                    "useruri": rail.result('get_user_details')["userDetails"]["uri"],
                    "supervisor_uri": rail.result('get_effective_supervisor_of_user')['supervisor']["uri"]
                        if rail.result('get_effective_supervisor_of_user') else ""
                }
            )

            if_performance_manager_details_in_feed = rail.IfOperator(
                task_id="if_performance_manager_details_in_feed",
                test=lambda dag_run: bool(dag_run.conf["performance_manager"]),
                yes_task="write_performance_manager_pending_logs",
            )

            write_performance_manager_pending_logs = rail.WriteLogOperator(
                task_id="write_performance_manager_pending_logs",
                log='{{dag_run.conf.supervisor_log}}',
                message="Supervisor",
                severity="Pending",
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf["employee_id"],
                    "reporting_manager": dag_run.conf["performance_manager"],
                    "reporting_manager_effective_date": null,
                    "Add_Update": "Update",
                    "type": "performance_manager",
                    "useruri": rail.result('get_user_details')["userDetails"]["uri"]
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{dag_run.conf.user_log}}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    "employee_id": "{{dag_run.conf.employee_id}}",
                    "action": "Update",
                    'status': 'Error',
                    'details': '{{ get_error_message() }}'
                },
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> get_user_details
            get_user_details >> if_user_exists_in_replicon >> rail.Label(
                'Yes') >> get_existing_worker_subtype
            if_user_exists_in_replicon >> rail.Label(
                'No') >> log_user_not_present_in_replicon >> catch_and_log_errors
            get_existing_worker_subtype >> if_worker_subtype_is_regular
            if_worker_subtype_is_regular >> rail.Label(
                'Yes') >> get_effective_user_group_membership
            if_worker_subtype_is_regular >> rail.Label(
                'No') >> if_worker_subtype_is_subcontractor
            if_worker_subtype_is_subcontractor >> rail.Label(
                'Yes') >> if_subcontractor_user_is_enabled
            if_subcontractor_user_is_enabled >> rail.Label(
                'Yes') >> compute_subcontractor_end_date >> update_subcontractor_user >> \
                log_subcontractor_disabled_exception >> catch_and_log_errors
            if_subcontractor_user_is_enabled >> rail.Label(
                'No') >> update_disabled_subcontractor_end_date >> log_subcontractor_already_disabled_exception >> catch_and_log_errors
            if_worker_subtype_is_subcontractor >> rail.Label(
                'No') >> if_new_is_agency_temp_from_subcontractor
            if_new_is_agency_temp_from_subcontractor >> rail.Label(
                'Yes') >> if_email_domain_matched
            if_new_is_agency_temp_from_subcontractor >> rail.Label(
                'No') >> if_worker_subtype_is_other
            if_worker_subtype_is_other >> rail.Label(
                'Yes') >> if_email_domain_matched
            if_email_domain_matched >> rail.Label('Yes') >> get_effective_user_group_membership >> get_placeholder_policyset >>\
                get_applicable_timeoff_types >> get_all_applicable_timeoff_types >> log_existing_timeoff_policies_for_user >>\
                list_uri_for_existing_timeoff_policies >> final_timeoff_policy_payload_variable >> if_fte_100_timeoffs_to_assign
            if_fte_100_timeoffs_to_assign >> rail.Label(
                'yes') >> foreach_applicable_timeoff_types_fte_100
            if_fte_100_timeoffs_to_assign >> rail.Label(
                'no') >> if_fte_any_timeoffs_to_assign

            foreach_applicable_timeoff_types_fte_100 >> default_timeoff_policyset_for_timeoff_type >> existing_policysetschedule_if_timeoff_policy_is_existing_in_user >>\
                log_relevant_historical_policies

            log_relevant_historical_policies >> final_policyset_schedule_for_timeoff_type >>\
                add_policy_to_final_timeoff_policy_payload_variable >> foreach_applicable_timeoff_types_fte_100_end

            foreach_applicable_timeoff_types_fte_100 >> foreach_applicable_timeoff_types_fte_100_end >> if_fte_any_timeoffs_to_assign

            if_fte_any_timeoffs_to_assign >> rail.Label(
                'yes') >> foreach_applicable_timeoff_types_fte_any
            if_fte_any_timeoffs_to_assign >> rail.Label('no') >> update_user

            foreach_applicable_timeoff_types_fte_any >> existing_policysetschedule_fte_any_if_timeoff_policy_is_existing_for_user >>\
                log_relevant_historical_policies_fte_any_timeoff_type

            log_relevant_historical_policies_fte_any_timeoff_type >> final_policyset_schedule_for_fte_any_timeoff_type >>\
                add_fte_any_timeoff_policy_to_final_timeoff_policy_payload_variable >> foreach_applicable_timeoff_types_fte_any_end

            foreach_applicable_timeoff_types_fte_any >> foreach_applicable_timeoff_types_fte_any_end

            foreach_applicable_timeoff_types_fte_any_end >> update_user >> is_login_name_changed >>\
                rail.Label(
                    'yes') >> update_user_login_name >> write_user_sucessfully_updated
            is_login_name_changed >> rail.Label(
                'no') >> write_user_sucessfully_updated
            write_user_sucessfully_updated >> get_effective_supervisor_of_user >>\
                write_supervisor_pending_logs >> if_performance_manager_details_in_feed
            if_performance_manager_details_in_feed >> rail.Label(
                'Yes') >> write_performance_manager_pending_logs >> catch_and_log_errors
            if_email_domain_matched >> rail.Label(
                'No') >> log_user_not_updated_worker_subtype_email_domain_different >> catch_and_log_errors
            if_worker_subtype_is_other >> rail.Label(
                'No') >> log_user_not_updated_worker_subtype_different >> catch_and_log_errors

        add_dags.append(dag)

    return add_dags


rail.for_each_instance(create_child_dag)
