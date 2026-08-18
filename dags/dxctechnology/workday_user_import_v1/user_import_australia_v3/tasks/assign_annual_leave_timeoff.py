from datetime import timedelta
from json import dumps, loads
import rail

from dxctechnology.workday_user_import_v1.user_import_australia_v3.utils.custom_methods import company_code_aues_fte_less_than_one_timeoff_name_starts_with_aus_annual_leave_test
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_date_in_json, get_todays_minus_specified_days_date_in_json, get_json_date_from_date_str

null = None

def assign_annual_leave_timeoff(_group_id, task_identifier, config, get_json_conf):
    with rail.TaskGroup(group_id = _group_id, prefix_group_id=False):
        def is_name_starts_with_aus_annual_leave_test():
            if rail.result("for_each_timeoff")['name'].startswith("[AUS] Annual Leave") and \
                bool(rail.result("get_required_details_for_timeoff_assignment")['aus_annual_leave']):
                return True
            return False

        is_name_starts_with_aus_annual_leave = rail.IfOperator(
            task_id = f"is_name_starts_with_aus_annual_leave_{task_identifier}",
            test=is_name_starts_with_aus_annual_leave_test,
            yes_task=f"is_company_code_aues_and_fte_less_than_one_and_timeoff_name_starts_with_aus_annual_leave_{task_identifier}",
            no_task=f"is_ia_updated_{task_identifier}"
        )

        is_ia_updated = rail.IfOperator(
            task_id = f"is_ia_updated_{task_identifier}",
            test = lambda dag_run: dag_run.conf['is_ia_updated'] in [True, 'true', 'True'],
            yes_task = f"is_ia_1_{task_identifier}",
            no_task =f"get_default_timeoff_type_policy_schedule_for_user2_{task_identifier}"
        )

        is_ia_1 = rail.IfOperator(
            task_id = f"is_ia_1_{task_identifier}",
            test = lambda dag_run: dag_run.conf['is_ia'] in ['1',1],
            yes_task = f"trigger_ia_one_timeoff_assignment_{task_identifier}",
            no_task = f"trigger_ia_zero_timeoff_assignment_{task_identifier}"
        )

        trigger_ia_one_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = f"trigger_ia_one_timeoff_assignment_{task_identifier}",
            items=[1],
            trigger_dag_id=config.workday_user_import_ia_one_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "file_name": dag_run.conf['file_name'],
                "login_name": dag_run.conf['loginName'],
                "email_id": dag_run.conf['email_id'],
                "emp_id": dag_run.conf['emp_id'],
                "user_uri": dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "star_date": dag_run.conf['json_formatted_dates']['ia_start_date'],
                "country": dag_run.conf['country'],
                "personnel_subarea": "",
                "employee_group":"",
                "employee_subgroup": "",
                "contineous_service_date": dag_run.conf['json_formatted_dates']['hire_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "secondary_timeoff_uri": rail.result("timeoff_type_uri_to_use"),
                "policy": [],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['ia_start_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        add_dag_run_id_to_wait1 = rail.SetVariableOperator(
            task_id = f"add_dag_run_id_to_wait1_{task_identifier}",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result(trigger_ia_one_timeoff_assignment.task_id),
            append=True
        )

        trigger_ia_zero_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = f"trigger_ia_zero_timeoff_assignment_{task_identifier}",
            items=[1],
            trigger_dag_id=config.workday_user_import_ia_zero_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "file_name": dag_run.conf['file_name'],
                "login_name": dag_run.conf['loginName'],
                "email_id": dag_run.conf['email_id'],
                "emp_id": dag_run.conf['emp_id'],
                "user_uri": dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "star_date": dag_run.conf['hire_date'],
                "ia_end_date": dag_run.conf['json_formatted_dates']['ia_end_date'],
                "country": dag_run.conf['country'],
                "personnel_subarea": "",
                "employee_group":"",
                "employee_subgroup": "",
                "contineous_service_date": dag_run.conf['hire_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "secondary_timeoff_uri": rail.result("timeoff_type_uri_to_use"),
                "policy": [],
                "json_formatted_dates": {
                    "start_date": get_json_date_from_date_str(dag_run.conf['hire_date']),
                    "ia_end_date": dag_run.conf['json_formatted_dates']['ia_end_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        add_dag_run_id_to_wait2 = rail.SetVariableOperator(
            task_id = f"add_dag_run_id_to_wait2_{task_identifier}",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result(trigger_ia_zero_timeoff_assignment.task_id),
            append=True
        )

        is_company_code_aues_and_fte_less_than_one_and_timeoff_name_starts_with_aus_annual_leave = rail.IfOperator(
            task_id = f"is_company_code_aues_and_fte_less_than_one_and_timeoff_name_starts_with_aus_annual_leave_{task_identifier}",
            test=lambda dag_run: company_code_aues_fte_less_than_one_timeoff_name_starts_with_aus_annual_leave_test(
                            dag_run, rail.result("for_each_timeoff")['name']),
            yes_task=f"trigger_aus_annual_leave_parttime_timeoff_child_{task_identifier}",
            no_task=f"trigger_aus_annual_leave_timeoff_child_{task_identifier}"
        )

        trigger_aus_annual_leave_parttime_timeoff_child = rail.TriggerDagRunForEachItemOperator(
            task_id = f"trigger_aus_annual_leave_parttime_timeoff_child_{task_identifier}",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_annual_leave_parttime_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "timeoff_type_uri": rail.result('for_each_timeoff')['uri'],
                "caller": "update",
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "current_timeoff_policies": null,
                "timeoff_type_name": rail.result('for_each_timeoff')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "hire_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": rail.result("date_to_use") if rail.result("date_to_use") else get_todays_date_in_json(),
                    "schedule_change_date_today_minus_1": rail.result("date_to_use") if rail.result("date_to_use") else get_todays_minus_specified_days_date_in_json(1),
                    "continuous_service_date": null
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "Secondarytimeoffuri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), 'name', '[AUS] Annual Leave (part-time)', 'uri'),
                "secondary_timeoff_name": "[AUS] Annual Leave (part-time)",
                "other_data": get_json_conf(),
                "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_runid_to_wait = rail.SetVariableOperator(
            task_id = f"add_dag_runid_to_wait_{task_identifier}",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_long_service_leave_dag"),
            append=True
        )

        trigger_aus_annual_leave_timeoff_child = rail.TriggerDagRunForEachItemOperator(
            task_id = f"trigger_aus_annual_leave_timeoff_child_{task_identifier}",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_annual_leave_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "timeoff_type_uri": rail.result('for_each_timeoff')['uri'],
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "caller": "Add",
                "current_timeoff_policies": null,
                "timeoff_type_name": rail.result('for_each_timeoff')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "hire_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": rail.result("date_to_use") if rail.result("date_to_use") else get_todays_date_in_json(),
                    "schedule_change_date_today_minus_1": rail.result("date_to_use") if rail.result("date_to_use") else get_todays_minus_specified_days_date_in_json(1),
                    "continuous_service_date": null
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "Secondarytimeoffuri": rail.result('for_each_timeoff')['uri'],
                "secondary_timeoff_name": null,
                "other_data": get_json_conf(),
                "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_runid_to_wait2 = rail.SetVariableOperator(
            task_id = f"add_dag_runid_to_wait2_{task_identifier}",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_long_service_leave_dag"),
            append=True
        )

        get_default_timeoff_type_policy_schedule_for_user2 = rail.RepliconServiceOperator(
            task_id=f"get_default_timeoff_type_policy_schedule_for_user2_{task_identifier}",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                }
            }
        )

        has_any_policy_to_assign2 = rail.IfOperator(
            task_id = f"has_any_policy_to_assign2_{task_identifier}",
            test=lambda : bool(rail.result(get_default_timeoff_type_policy_schedule_for_user2.task_id) and\
                                rail.result(get_default_timeoff_type_policy_schedule_for_user2.task_id)[0]['policySet']),
            yes_task=f"put_user_timeoff_account_policyset_schedule2_{task_identifier}",
            no_task=f"for_each_timeoff_end_dummy2_{task_identifier}"
        )

        def get_put_user_timeoff_account_policyset_schedule_payload(dag_run):
            timeoff_policy = loads(dumps(rail.result(get_default_timeoff_type_policy_schedule_for_user2.task_id)
                                        ).replace("null", "\"effective\""
                                        ).replace("\"script\"", "\"scriptTarget\""
                                        ))
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                },
                "policySetScheduleEntries": timeoff_policy
            }

        put_user_timeoff_account_policyset_schedule2 = rail.RepliconServiceOperator(
            task_id=f"put_user_timeoff_account_policyset_schedule2_{task_identifier}",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_user_timeoff_account_policyset_schedule_payload
        )

        for_each_timeoff_end_dummy2 = rail.EmptyOperator(
            task_id= f"for_each_timeoff_end_dummy2_{task_identifier}"
        )

        is_name_starts_with_aus_annual_leave >> rail.Label("No") >> is_ia_updated >> rail.Label("No") >> get_default_timeoff_type_policy_schedule_for_user2
        get_default_timeoff_type_policy_schedule_for_user2 >> has_any_policy_to_assign2 >> rail.Label(
            "Yes") >> put_user_timeoff_account_policyset_schedule2 >> for_each_timeoff_end_dummy2
        has_any_policy_to_assign2 >> rail.Label("No") >> for_each_timeoff_end_dummy2
        is_ia_updated >> rail.Label("Yes") >> is_ia_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment >> add_dag_run_id_to_wait1 >> for_each_timeoff_end_dummy2
        is_ia_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> add_dag_run_id_to_wait2 >> for_each_timeoff_end_dummy2

        is_name_starts_with_aus_annual_leave >> rail.Label("Yes") >> is_company_code_aues_and_fte_less_than_one_and_timeoff_name_starts_with_aus_annual_leave

        is_company_code_aues_and_fte_less_than_one_and_timeoff_name_starts_with_aus_annual_leave >> rail.Label(
            "Yes") >> trigger_aus_annual_leave_parttime_timeoff_child >> add_dag_runid_to_wait >> for_each_timeoff_end_dummy2
        
        is_company_code_aues_and_fte_less_than_one_and_timeoff_name_starts_with_aus_annual_leave >> rail.Label(
            "No") >> trigger_aus_annual_leave_timeoff_child >> add_dag_runid_to_wait2 >> for_each_timeoff_end_dummy2
        
        return is_name_starts_with_aus_annual_leave, for_each_timeoff_end_dummy2
