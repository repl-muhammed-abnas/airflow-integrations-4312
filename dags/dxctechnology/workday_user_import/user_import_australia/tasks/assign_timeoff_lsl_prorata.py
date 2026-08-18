from datetime import timedelta
from json import dumps, loads
import rail

from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_todays_date_in_json, get_json_date_from_date_str

null = None

def assign_lsl_prorata_timeoff(_group_id, task_identifier, config, get_json_conf: callable):
    
    with rail.TaskGroup(group_id=_group_id, prefix_group_id = False):
        def get_secondary_timeoff_uri_callble(dag_run):
            return rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), 'name', f"""[AUS] LSL Prorata {dag_run.conf['state']}""", 'uri')

        get_secondary_timeoff_uri = rail.PythonOperator(
            task_id = f"get_secondary_timeoff_uri_{task_identifier}",
            python_callable=get_secondary_timeoff_uri_callble
        )

        is_secondary_uri_present = rail.IfOperator(
            task_id = f"is_secondary_uri_present_{task_identifier}",
            test=lambda: bool(rail.result(get_secondary_timeoff_uri.task_id)),
            yes_task= f"is_aus_lsl_already_assigned_not_present_{task_identifier}",
            no_task=f"fail_dag_run_{task_identifier}"
        )

        fail_dag_run = rail.FailOperator(
            task_id = f"fail_dag_run_{task_identifier}",
            message="Placeholder timeoff type with '[AUS] LSL Prorata {{dag_run.conf.state}}' is not present"
        )

        is_aus_lsl_already_assigned_not_present = rail.IfOperator(
            task_id = f"is_aus_lsl_already_assigned_not_present_{task_identifier}",
            test= lambda: not rail.find_first_by_attr_and_get_attr(
                rail.result("get_required_details_for_timeoff_assignment")['currently_assigned_enabled_timeoffs'],
                "name",
                "[AUS] LSL Prorata Accrual"
            ),
            yes_task=f"is_ia_updated_{task_identifier}",
            no_task=f"trigger_aus_lsl_protata_timeoff_assignment_{task_identifier}"
        )

        is_ia_updated = rail.IfOperator(
            task_id = f"is_ia_updated_{task_identifier}",
            test = lambda dag_run: dag_run.conf['ia_updated'] in [True, 'true', 'True'],
            yes_task = f"is_ia_1_{task_identifier}",
            no_task =f"get_default_timeoff_type_policy_schedule_for_user_{task_identifier}"
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
                "star_date": dag_run.conf['ia_start_date'],
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
                    "start_date": get_json_date_from_date_str(dag_run.conf['ia_start_date'])
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
                "ia_end_date": dag_run.conf['ia_end_date'],
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
                    "ia_end_date": get_json_date_from_date_str(dag_run.conf['ia_end_date'])
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

        get_default_timeoff_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id=f"get_default_timeoff_type_policy_schedule_for_user_{task_identifier}",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result(get_secondary_timeoff_uri.task_id)
                }
            }
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = f"has_any_policy_to_assign_{task_identifier}",
            test=lambda : bool(rail.result(f"get_default_timeoff_type_policy_schedule_for_user_{task_identifier}") and rail.result(f"get_default_timeoff_type_policy_schedule_for_user_{task_identifier}")[0]['policySet']),
            yes_task=f"put_user_timeoff_account_policyset_schedule_{task_identifier}",
            no_task=f"for_each_timeoff_end_dummy_{task_identifier}"
        )

        def get_put_user_timeoff_account_policyset_schedule_payload(dag_run):
            timeoff_policy = loads(dumps(rail.result(f"get_default_timeoff_type_policy_schedule_for_user_{task_identifier}")
                                        ).replace("/null/", "\"effective\""
                                        ).replace("\"script\"", "\"scriptTarget\""
                                        ).replace("null", "\"effective\""))
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                },
                "policySetScheduleEntries": timeoff_policy
            }

        put_user_timeoff_account_policyset_schedule = rail.RepliconServiceOperator(
            task_id=f"put_user_timeoff_account_policyset_schedule_{task_identifier}",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_user_timeoff_account_policyset_schedule_payload
        )

        trigger_aus_lsl_protata_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id=f"trigger_aus_lsl_protata_timeoff_assignment_{task_identifier}",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_lsl_protata_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "timeoff_type_uri": rail.result('for_each_timeoff')['uri'],
                "caller": "Add",
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "current_timeoff_policies": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_user_timeoff_policy_summary"),
                    "timeOffType.name",
                    "[AUS] LSL Pro rata Accrual",
                    "policySetSchedule"
                ),
                "timeoff_type_name": rail.result('for_each_timeoff')['name'],
                "json_formatted_dates": {
                    "start_date": get_todays_date_in_json(),
                    "hire_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": rail.result("date_to_use") if rail.result("date_to_use") else (dag_run.conf['locationeffectivedate'] if dag_run.conf['location_updated'] == "yes" else get_todays_date_in_json())
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "Secondarytimeoffuri": rail.result(get_secondary_timeoff_uri.task_id),
                "secondary_timeoff_name": f"[AUS] LSL Prorata {dag_run.conf['state']}",
                "other_data": get_json_conf(),
                "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_run_id_to_wait = rail.SetVariableOperator(
            task_id = f"add_dag_run_id_to_wait_{task_identifier}",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result(trigger_aus_lsl_protata_timeoff_assignment.task_id),
            append=True
        )

        for_each_timeoff_end_dummy = rail.EmptyOperator(
            task_id = f"for_each_timeoff_end_dummy_{task_identifier}"
        )

        get_secondary_timeoff_uri >> is_secondary_uri_present >> rail.Label("no") >> fail_dag_run >> for_each_timeoff_end_dummy
        get_secondary_timeoff_uri >> is_secondary_uri_present >> rail.Label("yes") >> is_aus_lsl_already_assigned_not_present >> rail.Label(
                "Yes")  >>is_ia_updated >> rail.Label("No") >> get_default_timeoff_type_policy_schedule_for_user >> has_any_policy_to_assign >> rail.Label(
                    "Yes") >> put_user_timeoff_account_policyset_schedule >> for_each_timeoff_end_dummy
        is_ia_updated >> rail.Label("Yes") >> is_ia_1 >> trigger_ia_one_timeoff_assignment >> add_dag_run_id_to_wait1 >> for_each_timeoff_end_dummy
        is_ia_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> add_dag_run_id_to_wait2 >> for_each_timeoff_end_dummy
        has_any_policy_to_assign >> rail.Label("No") >> for_each_timeoff_end_dummy 
        is_aus_lsl_already_assigned_not_present >> rail.Label(
            "No") >> trigger_aus_lsl_protata_timeoff_assignment >> add_dag_run_id_to_wait >> for_each_timeoff_end_dummy
        
        return get_secondary_timeoff_uri, for_each_timeoff_end_dummy