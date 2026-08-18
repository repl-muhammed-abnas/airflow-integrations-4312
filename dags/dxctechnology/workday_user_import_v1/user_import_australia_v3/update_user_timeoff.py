from datetime import timedelta
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable
from tomlkit import date
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_minus_specified_days_date_in_json,\
    get_todays_date_in_json, get_json_date_from_date_str, get_required_formatted_date_from_json_date
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import get_specified_json_date_minus_specified_days_months_years_date_in_json
from dxctechnology.workday_user_import_v1.user_import_australia_v3.utils.custom_methods import get_trigger_id_callable,\
                                is_fte_based_timeoff_calculation_present_test
from dxctechnology.workday_user_import_v1.user_import_global_v2.utils import custom_methods as gbl_custom_methods  
from dxctechnology.workday_user_import_v1.user_import_australia_v3.tasks.assign_timeoff_lsl_prorata import assign_lsl_prorata_timeoff
from dxctechnology.workday_user_import_v1.user_import_australia_v3.tasks.assign_annual_leave_timeoff import assign_annual_leave_timeoff
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import convert_json_date_to_date, get_json_date_from_date

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_australia_users_update_user_timeoff_process_child_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.timeoff_process_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_australia, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="set_variable_to_store_run_id"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="set_variable_to_store_run_id",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        set_variable_to_store_run_id = rail.GetVariableOperator(
            task_id = "set_variable_to_store_run_id",
            name="variable_to_store_run_id"
        )

        def date_to_use_callable(dag_run):
            if dag_run.conf['is_ia_updated'] == "Yes":
                if dag_run.conf['is_ia'] == "1":
                    if "home pay" in dag_run.conf['assignment_type']:
                        return dag_run.conf['ia_start_date']
                if dag_run.conf['is_ia'] == "0":
                    return get_json_date_from_date(convert_json_date_to_date(dag_run.conf['ia_end_date']) + timedelta(days=1))
            return None

        date_to_use = rail.PythonOperator(
            task_id = "date_to_use",
            python_callable=date_to_use_callable
        )
        
        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id = "get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )
        
        get_user_timeoff_policy_summary = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri" : "{{dag_run.conf.user_uri}}"
            }
        )

        def get_assigned_timeoff_to_ignore(dag_run, user_timeoff_policy_summary, timeoff_to_ignore):
            data = []
            for timeoff in user_timeoff_policy_summary['policiesByTimeOffType']:
                ignore= rail.find_first_by_attr_and_get_attr(timeoff_to_ignore, 'URI', timeoff['timeOffType']['name'], default=None)
                data.append({
                    "name": timeoff['timeOffType']["name"],
                    "enabled": timeoff["isTimeOffAllowedAgainstThisTimeOffType"],
                    "uri":timeoff["timeOffType"]['uri'],
                    "policy":timeoff["policySetSchedule"],
                    "ignore": ('No' if ignore else 'Yes') if str(timeoff["isTimeOffAllowedAgainstThisTimeOffType"]).lower() == 'true' else "Yes"
                })
            
            return data, list(filter(lambda row: row['ignore'] == 'No' , data)), list(filter(lambda row: row['enabled'] is True, data))

        def get_required_details_for_timeoff_assignment_callable(dag_run):
            country = dag_run.conf['country']
            state = dag_run.conf['state']
            
            timeoff_to_ignore = list(filter(lambda row: row['Type']=='Timeoff to Ignore' and
                                                    row['Function'] == 'Workday User Sync' and
                                                    row['Country'] == country, config.MAPPER))
            
            mapper_timeoff = dag_run.conf['timeoffs']
            
            all_timeoffs_details = rail.result("get_all_timeoffs")
            
            default_timeoff = list(filter(lambda row: row['Type']=='Timeoff Sample' and row['Source']==state, config.MAPPER))
            
            if not default_timeoff:
                default_timeoff = [{}]
            
            timeoff_to_assign = [_timeoff['Value'] for _timeoff in mapper_timeoff]
            
            if not dag_run.conf['ausjc']:
                timeoff_to_assign.append(default_timeoff[0].get('Value'))

            user_timeoff_policy_summary = rail.result("get_user_timeoff_policy_summary")
            
            # Step 12: assigned_timeoff_data_to_ignore
            # Step 19: currently_assigned_enabled_timeoffs
            _all_assigned_timeoff_data, assigned_timeoff_data_to_ignore, currently_assigned_enabled_timeoffs = get_assigned_timeoff_to_ignore(
                dag_run, user_timeoff_policy_summary, timeoff_to_ignore)
            
            if assigned_timeoff_data_to_ignore:
                timeoff_to_assign.extend([to['name'] for to in assigned_timeoff_data_to_ignore])
            
            #Step 15
            _timeoff_name_and_uri = list(map(lambda to_name: {
                "name": to_name,
                "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_details, 'name', to_name, 'uri', None)
            }, timeoff_to_assign))
            
            # Step 16 & 21
            timeoff_uris = [tnau['uri'] for tnau in _timeoff_name_and_uri if tnau['uri']]

            # step 20
            def lsl_assigned_test(__item):
                if __item['name'].startswith('[AUS] LSL'):
                    if "Prorata Accrual" not in __item['name']:
                        if rail.find_first_by_attr_and_get_attr(_timeoff_name_and_uri, 'name', __item['name'], default=None):
                            return "No"
                        return "Yes"
                return "No"
            lsl_assigned = list(filter(lambda item: item['_assigned'] == "Yes" ,map(lambda record: {
                **record,
                **{
                    "_assigned": lsl_assigned_test(record)
                }
            }, currently_assigned_enabled_timeoffs)))

            # step 22
            timeoff_name_and_uri = list(map(lambda to_uri_name:
                {
                    "name": to_uri_name['name'],
                    "uri": to_uri_name['uri'],
                    "enabled": rail.find_first_by_attr_and_get_attr(_all_assigned_timeoff_data, 'uri', to_uri_name['uri'], 'enabled', None),
                    "status": "Yes" if rail.find_first_by_attr_and_get_attr(_all_assigned_timeoff_data, 'uri', to_uri_name['uri'], 'name') else "No"
                }, 
                _timeoff_name_and_uri))
            
            timeoff_name_and_uri_loop = list(filter(lambda timeoff_item: timeoff_item['status'] == "No", map(lambda to_uri_name:
                {
                    "name": to_uri_name['name'],
                    "uri": to_uri_name['uri'],
                    "enabled": rail.find_first_by_attr_and_get_attr(_all_assigned_timeoff_data, 'uri', to_uri_name['uri'], 'enabled', None),
                    "status": "Yes" if rail.find_first_by_attr_and_get_attr(_all_assigned_timeoff_data, 'uri', to_uri_name['uri'], 'name') else "No"
                }, 
                _timeoff_name_and_uri)))

            # step 23
            # _aad = _all_assigned_data
            all_assigned_data = list(map(lambda _aad: {
                    **_aad,
                    **{
                        "status": "Yes" if _aad['uri'] in timeoff_uris else "No"
                    }
            }, _all_assigned_timeoff_data ))

            rail.set_result(key="can_assign_timeoff", val=bool(timeoff_uris))

            aus_annual_leave, aus_lsl_leave, aus_lsl_prorata_leave = [], [], []

            for assigned_timeoff in _all_assigned_timeoff_data:
                if assigned_timeoff['name'] == '[AUS] Annual Leave':
                    aus_annual_leave.append(assigned_timeoff['uri'])
                    continue
                if assigned_timeoff['name'].startswith('[AUS] LSL'):                    
                    aus_lsl_leave.append(assigned_timeoff['uri'])
                    continue
                if assigned_timeoff['name'].startswith('[AUS] LSL Pro rata Accrual'):
                    aus_lsl_prorata_leave.append(assigned_timeoff['uri'])
                    continue
            return {
                "aus_annual_leave": aus_annual_leave,
                "aus_lsl_leave": aus_lsl_leave,
                "aus_lsl_prorata_leave": aus_lsl_prorata_leave,
                "timeoff_to_assign": timeoff_to_assign,
                "all_assigned_data": all_assigned_data,
                "to_name_and_uri": timeoff_name_and_uri,
                "timeoff_name_and_uri_loop": timeoff_name_and_uri_loop,
                "mapper_timeoff": mapper_timeoff,
                "timeoff_to_ignore": timeoff_to_ignore,
                "timeoff_uris": timeoff_uris,
                "timeoff_to_disable": list(filter(lambda to: to['status'] == 'No', all_assigned_data)),
                "timeoff_to_disable_with_policy": list(filter(lambda to: to['status'] == 'No' and bool(to['policy']) , all_assigned_data)),
                "currently_assigned_enabled_timeoffs": currently_assigned_enabled_timeoffs,
                "lsl_assigned": lsl_assigned
            }

        get_required_details_for_timeoff_assignment = rail.PythonOperator(
            task_id = "get_required_details_for_timeoff_assignment",
            python_callable=get_required_details_for_timeoff_assignment_callable
        )

        is_rehire = rail.IfOperator(
            task_id = "is_rehire",
            test = lambda dag_run: dag_run.conf['rehire'] and dag_run.conf['rehire'] == "Yes",
            yes_task = "trigger_rehire_timeoff_assignment",
            no_task = "is_fte_updated"
        )

        def get_rehire_timeoff_types():
            return [row for row in rail.result("get_required_details_for_timeoff_assignment")["all_assigned_data"] if row['policy']]

        def get_json_conf():
            dag_run_conf = rail.get_dag_run_conf()
            return rail.write_json_artifact(dag_run_conf)

        trigger_rehire_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_rehire_timeoff_assignment",
            trigger_dag_id = config.workday_user_import_australia_users_update_user_rehire_timeoff_process_child_dag,
            items=get_rehire_timeoff_types,
            conf= lambda dag_run, item : {
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "timeoff_type_uri": item['uri'],
                "current_timeoff_policies": item['policy'],
                "timeoff_type_name": item['name'],
                "json_formatted_dates": {
                    "start_date": gbl_custom_methods.get_todays_date_in_json(),
                    "continuous_service_date": dag_run.conf['json_formatted_dates']['service_date']
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "other_data": get_json_conf(),
                "fte": dag_run.conf['fte'] if dag_run.conf['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        wait_for_trigger_rehire_timeoff_assignment = rail.WaitForDagRunsSensor(
            task_id = "wait_for_trigger_rehire_timeoff_assignment",
            dag_runs="{{result('trigger_rehire_timeoff_assignment')}}",
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        is_fte_updated = rail.IfOperator(
            task_id = "is_fte_updated",
            test = lambda dag_run: rail.result('get_required_details_for_timeoff_assignment')['timeoff_to_assign'] and dag_run.conf['fte_updated'] == "Yes",
            yes_task = "for_each_all_assigned_timeoff_data",
            no_task = "is_any_timeoff_name_and_uri_present"
        )

        for_each_all_assigned_timeoff_data = rail.ForEachOperator(
            task_id = "for_each_all_assigned_timeoff_data",
            items=lambda: rail.result("get_required_details_for_timeoff_assignment")['all_assigned_data'],
            start_task="get_trigger_id",
            end_task="for_each_end"
        )

        get_trigger_id = rail.PythonOperator(
            task_id = "get_trigger_id",
            python_callable=lambda dag_run: get_trigger_id_callable(dag_run, config)
        )

        is_trigger_id_present = rail.IfOperator(
            task_id = "is_trigger_id_present",  
            test = lambda: bool(rail.result("get_trigger_id")),
            yes_task = "trigger_timeoff_assignment",
            no_task = "for_each_end"
        )

        def get_annual_part_time_leave_details():
            return rail.find_first_by_attr_and_get_attr(
                rail.result(get_all_timeoffs.task_id), 'name', '[AUS] Annual Leave (part-time)', 'uri', default={})

        def get_trigger_timeoff_assignment(dag_run):
            policy_set = rail.result('for_each_all_assigned_timeoff_data')['policy']
            if rail.result("get_trigger_id") == config.workday_user_import_australia_users_aus_annual_leave_parttime_timeoff_assignment_child_dag:
                policy_set = null
            return  {
                "timeoff_type_uri": rail.result('for_each_all_assigned_timeoff_data')['uri'],
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "caller": "Update",
                "current_timeoff_policies": policy_set,
                "timeoff_type_name": rail.result('for_each_all_assigned_timeoff_data')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": rail.result("date_to_use") if rail.result("date_to_use") else gbl_custom_methods.get_todays_date_in_json()
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "Secondarytimeoffuri": get_annual_part_time_leave_details(),
                "other_data": get_json_conf(),
                "fte": dag_run.conf['fte']
            }

        trigger_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_timeoff_assignment",
            items=[1],
            trigger_dag_id=lambda: rail.result("get_trigger_id"),
            conf= get_trigger_timeoff_assignment,
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_run_id_to_wait4 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait4",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_timeoff_assignment"),
            append=True
        )

        for_each_end = rail.EmptyOperator(
            task_id = "for_each_end"
        )

        is_any_timeoff_name_and_uri_present = rail.IfOperator(
            task_id = "is_any_timeoff_name_and_uri_present",
            test=lambda: bool(rail.result("get_required_details_for_timeoff_assignment")['timeoff_name_and_uri_loop']),
            yes_task="assign_timeoffs",
            no_task="process_timeoff_disable"
        )

        assign_timeoffs = rail.RepliconServiceOperator(
            task_id="assign_timeoffs",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "timeOffTypeUris": rail.result("get_required_details_for_timeoff_assignment")['timeoff_uris']
            }
        )

        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items=lambda: [item for item in rail.result("get_required_details_for_timeoff_assignment")['timeoff_name_and_uri_loop'] if item['uri']],
            start_task="is_fte_less_than_1",
            end_task="for_each_timeoff_end"
        )

        is_fte_less_than_1 = rail.IfOperator(
            task_id = "is_fte_less_than_1",
            test=lambda dag_run: float(dag_run.conf['fte'] if dag_run.conf['fte'] else 0) < 1.0,
            yes_task="is_fte_based_timeoff_calculation_present",
            no_task="empty_ftp_not_less_than_1"
        )

        is_fte_based_timeoff_calculation_present = rail.IfOperator(
            task_id = "is_fte_based_timeoff_calculation_present",
            test=lambda: is_fte_based_timeoff_calculation_present_test(
                to_name=rail.result("for_each_timeoff")['name'],
                config=config
            ),
            yes_task="trigger_aus_personal_carers_leave_parttime_child",
            no_task="is_timeoff_long_service_leave"
        )

        trigger_aus_personal_carers_leave_parttime_child = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_aus_personal_carers_leave_parttime_child",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_personal_carers_leave_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "timeoff_type_uri": rail.result('for_each_timeoff')['uri'],
                "caller": "Add", # as per workato
                "actual_caller": "update",
                "policy_sets": dumps([]),
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "current_timeoff_policies": null,
                "timeoff_type_name": rail.result('for_each_timeoff')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": rail.result("date_to_use") if rail.result("date_to_use") else gbl_custom_methods.get_todays_date_in_json()
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "Secondarytimeoffuri": get_annual_part_time_leave_details(),
                "other_data": get_json_conf(),
                "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_run_id_to_wait1 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait1",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_aus_personal_carers_leave_parttime_child"),
            append=True
        ) 

        def is_timeoff_long_service_leave_test():
            name:str = rail.result("for_each_timeoff")['name']
            if name.startswith("[AUS] LSL") and "[AUS] LSL Prorata" not in name:
                return True
            return False

        is_timeoff_long_service_leave = rail.IfOperator(
            task_id = "is_timeoff_long_service_leave",
            test=is_timeoff_long_service_leave_test,
            yes_task="is_uri_value_present",
            no_task="is_timeoff_name_starts_with_aus_lsl_prorata"
        )

        def is_uri_value_present_test(dag_run):
            if rail.result("get_required_details_for_timeoff_assignment", "lsl_assigned"):
                return True
            return False

        is_uri_value_present = rail.IfOperator(
            task_id = "is_uri_value_present",
            test=is_uri_value_present_test,
            yes_task="trigger_long_service_leave_timeoff_assignment",
            no_task="is_ia_updated"
        )

        is_ia_updated = rail.IfOperator(
            task_id = "is_ia_updated",
            test = lambda dag_run: dag_run.conf['is_ia_updated'] in [True, 'true', 'True'],
            yes_task = "is_ia_1",
            no_task = "get_default_timeoff_schedule_policy_for_user"
        )

        is_ia_1 = rail.IfOperator(
            task_id = "is_ia_1",
            test = lambda dag_run: dag_run.conf['is_ia'] in ['1',1],
            yes_task = "trigger_ia_one_timeoff_assignment",
            no_task = "trigger_ia_zero_timeoff_assignment"
        )

        trigger_ia_one_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_ia_one_timeoff_assignment",
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
                "star_date": get_json_date_from_date_str(dag_run.conf['ia_start_date']),
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

        add_dag_run_id_to_wait7 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait7",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_one_timeoff_assignment"),
            append=True
        )

        trigger_ia_zero_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_ia_zero_timeoff_assignment",
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
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "ia_end_date": dag_run.conf['json_formatted_dates']['ia_end_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        add_dag_run_id_to_wait8 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait8",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_zero_timeoff_assignment"),
            append=True
        )

        get_default_timeoff_schedule_policy_for_user = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_schedule_policy_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.user_uri }}",
                    "timeOffTypeUri": "{{ result('for_each_timeoff').uri }}"
                }
            }
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = f"has_any_policy_to_assign",
            test=lambda : bool(rail.result("get_default_timeoff_schedule_policy_for_user") and\
                                rail.result("get_default_timeoff_schedule_policy_for_user")[0]['policySet']),
            yes_task=f"put_user_timeoff_account_policyset_schedule",
            no_task=f"for_each_timeoff_end"
        )

        def get_put_user_timeoff_account_policyset_schedule_payload(dag_run):
            timeoff_policy = loads(dumps(rail.result("get_default_timeoff_schedule_policy_for_user")
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

        put_user_timeoff_account_policyset_schedule = rail.RepliconServiceOperator(
            task_id=f"put_user_timeoff_account_policyset_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_user_timeoff_account_policyset_schedule_payload
        )

        trigger_long_service_leave_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_long_service_leave_timeoff_assignment",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_long_service_leave_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "timeoff_type_uri": rail.result('for_each_timeoff')['uri'],
                "caller": "Update",
                "current_timeoff_policies": null,
                "timeoff_type_name": rail.result('for_each_timeoff')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": rail.result("date_to_use") if rail.result("date_to_use") else (dag_run.conf['locationeffectivedate'] if dag_run.conf['location_updated'] == "yes" else gbl_custom_methods.get_todays_date_in_json()),
                    "location_effective_date": dag_run.conf['locationeffectivedate'],
                    "2_months_before_location_effective_date": get_specified_json_date_minus_specified_days_months_years_date_in_json(
                                                                        dag_run.conf['locationeffectivedate'],
                                                                        months_in_number=2)
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "Secondarytimeoffuri": rail.result("get_required_details_for_timeoff_assignment", "lsl_assigned")[0]['uri'],
                "other_data": get_json_conf(),
                "fte": (dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0),
                "aus_prorata_accrual_uri": rail.find_first_by_attr_and_get_attr(
                                                rail.result(get_user_timeoff_policy_summary.task_id)['get_user_timeoff_policy_summary'],
                                                'timeOffType.displayText',
                                                '[AUS] LSL Prorata Accrual',
                                                'uri',
                                                default=''
                                            ),
                "old_location_state": dag_run.conf['current_assigned_location_state'],
                "currently_assigned_lsl_timeoff_uri": rail.result('') if rail.result('') else rail.result('for_each_timeoff')['uri'],
                "location_updated": dag_run.conf['location_updated'] and dag_run.conf['location_updated'].lower() == "yes",
                "lsl_anniversary_date": dag_run.conf['lsl_anniversary_date'] if dag_run.conf['lsl_anniversary_date'] else dag_run.conf['json_formatted_dates']['hire_date'],
                "lsl_anniversary_date_json": dag_run.conf['lsl_anniversary_date_json'] if dag_run.conf['lsl_anniversary_date'] else dag_run.conf['hire_date_json']
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_run_id_to_wait2 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait2",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_long_service_leave_timeoff_assignment"),
            append=True
        ) 

        get_secondary_timeoff_uri, for_each_timeoff_dummy = assign_lsl_prorata_timeoff('assign_lsl_prorata_timeoff_not_leave', 'lsl_prorata1', config, get_json_conf)

        # aalt1 = assign_annual_leave_timeoff_1
        is_name_starts_with_aus_annual_leave, for_each_timeoff_end_dummy2 = assign_annual_leave_timeoff("assign_annual_leave_timeoff", 'aalt1', config, get_json_conf)

        is_timeoff_name_starts_with_aus_lsl_prorata = rail.IfOperator(
            task_id = "is_timeoff_name_starts_with_aus_lsl_prorata",
            test=lambda: rail.result("for_each_timeoff")['name'].startswith("[AUS] LSL Prorata"),
            yes_task=get_secondary_timeoff_uri.task_id,
            no_task=is_name_starts_with_aus_annual_leave.task_id
        )

        empty_ftp_not_less_than_1 = rail.EmptyOperator(
            task_id = "empty_ftp_not_less_than_1"
        )

        def is_name_starts_with_aus_lsl_and_lsl_timeoff_assigned_and_and_timeoff_name_not_lsl_prorata_test(dag_run):
            name:str = rail.result("for_each_timeoff")['name']
            if name.startswith("[AUS] LSL") and "[AUS] LSL Prorata" not in name:
                return True
            return False

        is_name_starts_with_aus_lsl_and_lsl_timeoff_assigned_and_and_timeoff_name_not_lsl_prorata = rail.IfOperator(
            task_id = "is_name_starts_with_aus_lsl_and_lsl_timeoff_assigned_and_and_timeoff_name_not_lsl_prorata",
            test=is_name_starts_with_aus_lsl_and_lsl_timeoff_assigned_and_and_timeoff_name_not_lsl_prorata_test,
            yes_task = "is_uri_value_present2",
            no_task = "is_timeoff_name_starts_with_lsl_prorata"
        )

        is_uri_value_present2 = rail.IfOperator(
            task_id = "is_uri_value_present2",
            test=is_uri_value_present_test,
            yes_task="trigger_long_service_leave_dag",
            no_task="is_ia_updated_2"
        )

        is_ia_updated_2 = rail.IfOperator(
            task_id = "is_ia_updated_2",
            test = lambda dag_run: dag_run.conf['is_ia_updated'] in [True, 'true', 'True'],
            yes_task = "is_ia_equal_1",
            no_task = "get_default_timeoff_schedule_policy_for_user2"
        )

        is_ia_equal_1 = rail.IfOperator(
            task_id = "is_ia_equal_1",
            test = lambda dag_run: dag_run.conf['is_ia'] in ['1',1],
            yes_task = "trigger_ia_one_timeoff_assignment2",
            no_task = "trigger_ia_zero_timeoff_assignment2"
        )

        trigger_ia_one_timeoff_assignment2 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_ia_one_timeoff_assignment2",
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

        add_dag_run_id_to_wait9 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait9",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_zero_timeoff_assignment2"),
            append=True
        )

        trigger_ia_zero_timeoff_assignment2 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_ia_zero_timeoff_assignment2",
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
                    "ia_end_date": dag_run.conf['json_formatted_dates']['ia_end_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        add_dag_run_id_to_wait10 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait10",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_zero_timeoff_assignment2"),
            append=True
        )

        get_default_timeoff_schedule_policy_for_user2 = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_schedule_policy_for_user2",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.user_uri }}",
                    "timeOffTypeUri": "{{ result('for_each_timeoff').uri }}"
                }
            }
        )

        has_any_policy_to_assign2 = rail.IfOperator(
            task_id = f"has_any_policy_to_assign2",
            test=lambda : bool(rail.result("get_default_timeoff_schedule_policy_for_user2") and\
                                rail.result("get_default_timeoff_schedule_policy_for_user2")[0]['policySet']),
            yes_task=f"put_user_timeoff_account_policyset_schedule2",
            no_task=f"for_each_timeoff_end"
        )

        def get_put_user_timeoff_account_policyset_schedule_payload2(dag_run):
            timeoff_policy = loads(dumps(rail.result("get_default_timeoff_schedule_policy_for_user2")[0]['policySet']
                                        ).replace("/null/", "\"effective\""
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
            task_id=f"put_user_timeoff_account_policyset_schedule2",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_user_timeoff_account_policyset_schedule_payload2
        )

        trigger_long_service_leave_dag = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_long_service_leave_dag",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_long_service_leave_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "timeoff_type_uri": rail.result('for_each_timeoff')['uri'],
                "current_timeoff_policies": null,
                "timeoff_type_name": rail.result('for_each_timeoff')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": gbl_custom_methods.get_todays_date_in_json(),
                    "schedule_change_date_today_minus_1":get_todays_minus_specified_days_date_in_json(1),
                    "location_effective_date": dag_run.conf['locationeffectivedate'],
                    "2_months_before_location_effective_date": get_specified_json_date_minus_specified_days_months_years_date_in_json(
                                                                        dag_run.conf['locationeffectivedate'],
                                                                        months_in_number=2)
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "secondary_timeoffuri": rail.result("get_required_details_for_timeoff_assignment")['aus_lsl_leave'],
                "secondary_timeoffname": null,
                "other_data": get_json_conf(),
                "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0,
                "aus_prorata_accrual_uri": rail.find_first_by_attr_and_get_attr(
                                                rail.result(get_user_timeoff_policy_summary.task_id)['get_user_timeoff_policy_summary'],
                                                'timeOffType.displayText',
                                                '[AUS] LSL Prorata Accrual',
                                                'uri',
                                                default=''
                                            ),
                "old_location_state": dag_run.conf['current_assigned_location_state'],
                "currently_assigned_lsl_timeoff_uri": rail.result('') if rail.result('') else rail.result('for_each_timeoff')['uri'],
                "location_updated": dag_run.conf['location_updated'] and dag_run.conf['location_updated'].lower() == "yes",
                "lsl_anniversary_date": dag_run.conf['lsl_anniversary_date'] if dag_run.conf['lsl_anniversary_date'] else dag_run.conf['json_formatted_dates']['hire_date'],
                "lsl_anniversary_date_json": dag_run.conf['lsl_anniversary_date_json'] if dag_run.conf['lsl_anniversary_date'] else dag_run.conf['hire_date_json']
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_run_id_to_wait3 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait3",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_long_service_leave_dag"),
            append=True
        )

        get_secondary_timeoff_uri2, for_each_timeoff_dummy2 = assign_lsl_prorata_timeoff('assign_lsl_prorata_timeoff_fte_more_than_1', 'lsl_prorata2', config, get_json_conf)

        is_name_starts_with_aus_annual_leave2, for_each_timeoff_end_dummy3 = assign_annual_leave_timeoff("assign_annual_leave_timeoff2", 'aalt2', config, get_json_conf)

        is_timeoff_name_starts_with_lsl_prorata = rail.IfOperator(
            task_id = "is_timeoff_name_starts_with_lsl_prorata",
            test = lambda: rail.result("for_each_timeoff")['name'].startswith('[AUS] LSL Prorata'),
            yes_task=get_secondary_timeoff_uri2.task_id,
            no_task=is_name_starts_with_aus_annual_leave2.task_id
        )


        for_each_timeoff_end = rail.EmptyOperator(
            task_id = "for_each_timeoff_end"
        )

        is_location_updated = rail.IfOperator(
            task_id = "is_location_updated",
            test=lambda dag_run: dag_run.conf['location_updated'] and dag_run.conf['location_updated'].lower() == "yes",
            yes_task="for_each_timeoff_for_location",
            no_task="process_timeoff_disable"
        )

        for_each_timeoff_for_location = rail.ForEachOperator(
            task_id = "for_each_timeoff_for_location",
            items=lambda: rail.result("get_required_details_for_timeoff_assignment")['currently_assigned_enabled_timeoffs'],
            start_task="is_uri_present_in_timeoff_to_be_assigned_list",
            end_task="for_each_timeoff_for_location_end"
        )

        def is_uri_present_in_timeoff_to_be_assigned_list_test():
            # the timeoff that will be assigned will no be processed here
            return not (rail.find_first_by_attr_and_get_attr(rail.result('get_required_details_for_timeoff_assignment')['timeoff_name_and_uri_loop'],
                                                        'uri',
                                                        rail.result("for_each_timeoff_for_location")['uri'],
                                                        default=None))

        is_uri_present_in_timeoff_to_be_assigned_list = rail.IfOperator(
            task_id = "is_uri_present_in_timeoff_to_be_assigned_list",
            test=is_uri_present_in_timeoff_to_be_assigned_list_test,
            yes_task="is_timeoff_startswith_auslsl_and_not_auslsl_prorata_and_not_assigned",
            no_task="for_each_timeoff_for_location_end"
        )


        def is_timeoff_startswith_auslsl_and_not_auslsl_prorata_and_not_assigned_test():
            if rail.result("for_each_timeoff_for_location")['name'].startswith("[AUS] LSL"):
                if "[AUS] LSL Prorata" not in rail.result("for_each_timeoff_for_location")['name']:
                    assigned_lsl_value = rail.result('get_required_details_for_timeoff_assignment')['lsl_assigned'] or [{}]
                    if rail.result("for_each_timeoff_for_location")['name'] != assigned_lsl_value[0].get("name"):
                        return True
            return False

        is_timeoff_startswith_auslsl_and_not_auslsl_prorata_and_not_assigned = rail.IfOperator(
            task_id = "is_timeoff_startswith_auslsl_and_not_auslsl_prorata_and_not_assigned",
            test=is_timeoff_startswith_auslsl_and_not_auslsl_prorata_and_not_assigned_test,
            yes_task="is_uri_present_2",
            no_task="is_timeoff_name_starts_with_aus_lsl_prorata2"
        )

        is_uri_present_2 = rail.IfOperator(
            task_id = "is_uri_present_2",
            test=is_uri_value_present_test,
            yes_task="trigger_long_service_leave_dag2",
            no_task="is_ia_updated4"
        )

        trigger_long_service_leave_dag2 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_long_service_leave_dag2",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_annual_leave_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "timeoff_type_uri": rail.result('for_each_timeoff_for_location')['uri'],
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "caller": "Add",
                "current_timeoff_policies": null,
                "timeoff_type_name": rail.result('for_each_timeoff_for_location')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "hire_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": rail.result("date_to_use") if rail.result("date_to_use") else dag_run.conf['locationeffectivedate'],
                    "schedule_change_date_today_minus_1": rail.result("date_to_use") if rail.result("date_to_use") else dag_run.conf['locationeffectivedate'],
                    "continuous_service_date": null
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "Secondarytimeoffuri": rail.result('for_each_timeoff_for_location')['uri'],
                "secondary_timeoff_name":"NA",
                "other_data": get_json_conf(),
                "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_run_id_to_wait5 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait5",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_long_service_leave_dag2"),
            append=True
        )

        is_ia_updated4 = rail.IfOperator(
            task_id = "is_ia_updated4",
            test = lambda dag_run: dag_run.conf['is_ia_updated'] in [True, 'true', 'True'],
            yes_task = "is_ia_equal_1_4",
            no_task = "get_default_timeoff_schedule_policy_for_user3"
        )

        is_ia_equal_1_4 = rail.IfOperator(
            task_id = "is_ia_equal_1_4",
            test = lambda dag_run: dag_run.conf['is_ia'] in ['1',1],
            yes_task = "trigger_ia_one_timeoff_assignment4",
            no_task = "trigger_ia_zero_timeoff_assignment4"
        )

        trigger_ia_one_timeoff_assignment4 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_ia_one_timeoff_assignment4",
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
                "timeoff_uri": rail.result("for_each_timeoff_for_location")['uri'],
                "timeoff_name": rail.result("for_each_timeoff_for_location")['name'],
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

        add_dag_run_id_to_wait13 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait13",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_zero_timeoff_assignment4"),
            append=True
        )

        trigger_ia_zero_timeoff_assignment4 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_ia_zero_timeoff_assignment4",
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
                "timeoff_uri": rail.result("for_each_timeoff_for_location")['uri'],
                "timeoff_name": rail.result("for_each_timeoff_for_location")['name'],
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

        add_dag_run_id_to_wait14 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait14",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_zero_timeoff_assignment4"),
            append=True
        )

        empty_policy_updated2 = rail.EmptyOperator(
            task_id = "empty_policy_updated2"
        )


        get_default_timeoff_schedule_policy_for_user3 = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_schedule_policy_for_user3",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.user_uri }}",
                    "timeOffTypeUri": "{{ result('for_each_timeoff_for_location').uri }}"
                }
            }
        )

        has_any_policy_to_assign3 = rail.IfOperator(
            task_id = f"has_any_policy_to_assign3",
            test=lambda : bool(rail.result("get_default_timeoff_schedule_policy_for_user3") and\
                                rail.result("get_default_timeoff_schedule_policy_for_user3")['policySet']),
            yes_task=f"put_user_timeoff_account_policyset_schedule3",
            no_task=f"empty_policy_updated2"
        )

        def get_put_user_timeoff_account_policyset_schedule_payload3(dag_run):
            timeoff_policy = loads(dumps(rail.result("get_default_timeoff_schedule_policy_for_user3")['policySet']
                                        ).replace("/null/", "\"effective\""
                                        ).replace("\"script\"", "\"scriptTarget\""
                                        ))
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_for_location")['uri']
                },
                "policySetScheduleEntries": timeoff_policy
            }

        put_user_timeoff_account_policyset_schedule3 = rail.RepliconServiceOperator(
            task_id=f"put_user_timeoff_account_policyset_schedule3",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_user_timeoff_account_policyset_schedule_payload3
        )

        is_timeoff_name_starts_with_aus_lsl_prorata2 = rail.IfOperator(
            task_id = "is_timeoff_name_starts_with_aus_lsl_prorata2",
            test=lambda: rail.result("for_each_timeoff_for_location")['name'].startswith("[AUS] LSL Prorata"),
            yes_task="get_secondary_timeoff_uri3",
            no_task="for_each_timeoff_for_location_end"
        )

        def get_secondary_timeoff_uri_callble(dag_run):
            return rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), 'name', f"""[AUS] LSL Prorata {dag_run.conf['state']}""", 'uri')

        get_secondary_timeoff_uri3 = rail.PythonOperator(
            task_id = "get_secondary_timeoff_uri3",
            python_callable=get_secondary_timeoff_uri_callble
        )

        def is_aus_lsl_prorata_accrual_not_present_test():
            currently_assigned_enabled_timeoffs = rail.result("get_required_details_for_timeoff_assignment")['currently_assigned_enabled_timeoffs']
            if not currently_assigned_enabled_timeoffs:
                # nill not present => None not present => not bool(None) => True
                return True
            # not is as in workato its `not present` condition
            return not bool(rail.find_first_by_attr_and_get_attr(currently_assigned_enabled_timeoffs,
                                                      'name',
                                                      '[AUS] LSL Prorata Accrual',
                                                      'uri',
                                                      default=None))

        is_aus_lsl_prorata_accrual_not_present = rail.IfOperator(
            task_id = "is_aus_lsl_prorata_accrual_not_present",
            test=is_aus_lsl_prorata_accrual_not_present_test,
            yes_task="is_ia_updated3",
            no_task="is_aus_lsl_prorata_accrual_present"
        )

        is_ia_updated3 = rail.IfOperator(
            task_id = "is_ia_updated3",
            test = lambda dag_run: dag_run.conf['is_ia_updated'] in [True, 'true', 'True'],
            yes_task = "is_ia_equal_1_2",
            no_task = "get_default_timeoff_schedule_policy_for_user4"
        )

        is_ia_equal_1_2 = rail.IfOperator(
            task_id = "is_ia_equal_1_2",
            test = lambda dag_run: dag_run.conf['is_ia'] in ['1',1],
            yes_task = "trigger_ia_one_timeoff_assignment3",
            no_task = "trigger_ia_zero_timeoff_assignment3"
        )

        trigger_ia_one_timeoff_assignment3 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_ia_one_timeoff_assignment3",
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
                "timeoff_uri": rail.result("for_each_timeoff_for_location")['uri'],
                "timeoff_name": rail.result("for_each_timeoff_for_location")['name'],
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

        add_dag_run_id_to_wait11 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait11",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_zero_timeoff_assignment3"),
            append=True
        )

        trigger_ia_zero_timeoff_assignment3 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_ia_zero_timeoff_assignment3",
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
                "timeoff_uri": rail.result("for_each_timeoff_for_location")['uri'],
                "timeoff_name": rail.result("for_each_timeoff_for_location")['name'],
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

        add_dag_run_id_to_wait12 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait12",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_zero_timeoff_assignment3"),
            append=True
        )         

        get_default_timeoff_schedule_policy_for_user4 = rail.RepliconServiceOperator(   
            task_id = "get_default_timeoff_schedule_policy_for_user4",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.user_uri }}",
                    "timeOffTypeUri": "{{ result('get_secondary_timeoff_uri3') }}"
                }
            }
        )

        has_any_policy_to_assign4 = rail.IfOperator(
            task_id = f"has_any_policy_to_assign4",
            test=lambda : bool(rail.result("get_default_timeoff_schedule_policy_for_user4") and\
                                rail.result("get_default_timeoff_schedule_policy_for_user4")['policySet']),
            yes_task=f"put_user_timeoff_account_policyset_schedule4",
            no_task=f"empty_policy_updated"
        )

        def get_put_user_timeoff_account_policyset_schedule_payload4(dag_run):
            timeoff_policy = loads(dumps(rail.result("get_default_timeoff_schedule_policy_for_user4")['policySet']
                                        ).replace("/null/", "\"effective\""
                                        ).replace("\"script\"", "\"scriptTarget\""
                                        ))
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_for_location")['uri']
                },
                "policySetScheduleEntries": timeoff_policy
            }

        put_user_timeoff_account_policyset_schedule4 = rail.RepliconServiceOperator(
            task_id=f"put_user_timeoff_account_policyset_schedule4",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_user_timeoff_account_policyset_schedule_payload4
        )

        empty_policy_updated = rail.EmptyOperator(
            task_id = "empty_policy_updated"
        )

        is_aus_lsl_prorata_accrual_present = rail.IfOperator(
            task_id = "is_aus_lsl_prorata_accrual_present",
            test=lambda: not is_aus_lsl_prorata_accrual_not_present_test(),
            yes_task="trigger_aus_lsl_prorata_accrual_timeoff_assignment",
            no_task="for_each_timeoff_for_location_end"
        )

        trigger_aus_lsl_prorata_accrual_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_aus_lsl_prorata_accrual_timeoff_assignment",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_lsl_protata_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "timeoff_type_uri": rail.result('for_each_timeoff_for_location')['uri'],
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "caller": "Update",
                "current_timeoff_policies": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'],
                    "timeOffType.name",
                    "[AUS] LSL Prorata Accrual",
                    "policySetSchedule"
                ),
                "timeoff_type_name": rail.result('for_each_timeoff_for_location')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "hire_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": rail.result("date_to_use") if rail.result("date_to_use") else dag_run.conf['locationeffectivedate']
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "Secondarytimeoffuri": rail.result("get_secondary_timeoff_uri3"),
                "secondary_timeoff_name": f"[AUS] LSL Prorata {dag_run.conf['state']}",
                "other_data": get_json_conf(),
                "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_run_id_to_wait6 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait6",
            name=lambda : rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_aus_lsl_prorata_accrual_timeoff_assignment"),
            append=True
        )

        for_each_timeoff_for_location_end = rail.EmptyOperator(
            task_id = "for_each_timeoff_for_location_end"
        )

        def date_to_use_for_disable(dag_run, return_as_json_date=True):
            if dag_run.conf['is_ia_updated'] == "Yes":
                if dag_run.conf['is_ia'] == "1":
                    if return_as_json_date:
                        return get_json_date_from_date_str(dag_run.conf['ia_start_date'])
                    return dag_run.conf['ia_start_date']
                if dag_run.conf['is_ia'] == "0":
                    if not return_as_json_date:
                        return dag_run.conf['ia_end_date']
                    return get_json_date_from_date(convert_json_date_to_date(dag_run.conf['ia_end_date']) + timedelta(days=1))
            else:
                if dag_run.conf['location_updated'] == "yes":
                    if not return_as_json_date: 
                        return dag_run.conf['item']['locationeffectivedate']
                    return dag_run.conf['locationeffectivedate']
            if return_as_json_date:
                return get_todays_date_in_json()   
            return get_required_formatted_date_from_json_date(get_todays_date_in_json())

        process_timeoff_disable = rail.TriggerDagRunForEachItemOperator(
                task_id="process_timeoff_disable",
                items=lambda: rail.result(
                    "get_required_details_for_timeoff_assignment")['timeoff_to_disable_with_policy'],
                trigger_dag_id=config.process_time_off_accrual,
                conf=lambda dag_run, item: {
                    **{k: v for k,v in dag_run.conf.items() if k not in ['end_date']},
                    **{
                        "timeoff_type_uri": item['uri'],
                        "policy_set": dumps(item['policy']).replace("[[{", "[{").replace("}]]", "}]"),
                        "user_end_date_json": date_to_use_for_disable(dag_run),
                        "end_date": date_to_use_for_disable(dag_run, return_as_json_date=False),
                        "add_balance_as_zero": "yes",
                    }
                },
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )

        def gather_all_runids_to_wait_callable():
            dag_run_ids = rail.get_dag_run_var(rail.result("set_variable_to_store_run_id")['name'])
            if not dag_run_ids:
                dag_run_ids = []
            if rail.result("process_timeoff_disable"):
                dag_run_ids.extend(rail.result("process_timeoff_disable"))
            return [item for item in dag_run_ids if item is not None]

        gather_all_runids_to_wait = rail.PythonOperator(
            task_id = "gather_all_runids_to_wait",
            python_callable=gather_all_runids_to_wait_callable
        )

        wait_for_dag_run_to_complete = rail.WaitForDagRunsSensor(
            task_id = "wait_for_dag_run_to_complete",
            dag_runs="{{result('gather_all_runids_to_wait')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            # log="{{dag_run.conf.user_log}}",
            message = "User Update Error",
            severity = "Error",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['emp_id'],
                "Email": dag_run.conf['email_id'],
                "Action": "Update",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )


        is_location_updated >> rail.Label("No") >> process_timeoff_disable
        is_location_updated >> rail.Label("Yes") >> for_each_timeoff_for_location
        
        for_each_timeoff_for_location >> for_each_timeoff_for_location_end >> process_timeoff_disable
        for_each_timeoff_for_location >> is_uri_present_in_timeoff_to_be_assigned_list >> rail.Label("No") >> for_each_timeoff_for_location_end

        is_uri_present_in_timeoff_to_be_assigned_list >> rail.Label("Yes") >> is_timeoff_startswith_auslsl_and_not_auslsl_prorata_and_not_assigned
        is_timeoff_startswith_auslsl_and_not_auslsl_prorata_and_not_assigned >> rail.Label("Yes") >> is_uri_present_2 >> rail.Label("Yes") >> trigger_long_service_leave_dag2
        trigger_long_service_leave_dag2 >> add_dag_run_id_to_wait5 >> for_each_timeoff_for_location_end
        is_uri_present_2 >> rail.Label("No") >> is_ia_updated4 >> rail.Label("No") >> get_default_timeoff_schedule_policy_for_user3 >> has_any_policy_to_assign3 >> rail.Label("Yes") >> put_user_timeoff_account_policyset_schedule3
        put_user_timeoff_account_policyset_schedule3 >> empty_policy_updated2 >> for_each_timeoff_for_location_end
        has_any_policy_to_assign3 >> rail.Label("No") >> empty_policy_updated2 >> for_each_timeoff_for_location_end

        is_ia_updated4 >> rail.Label("Yes") >> is_ia_equal_1_4 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment4 >> add_dag_run_id_to_wait13 >> empty_policy_updated2
        is_ia_equal_1_4 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment4 >> add_dag_run_id_to_wait14 >> empty_policy_updated2
        
        is_timeoff_startswith_auslsl_and_not_auslsl_prorata_and_not_assigned >> rail.Label("No") >> is_timeoff_name_starts_with_aus_lsl_prorata2
        is_timeoff_name_starts_with_aus_lsl_prorata2 >> rail.Label("No") >> for_each_timeoff_for_location_end
        is_timeoff_name_starts_with_aus_lsl_prorata2 >> rail.Label("Yes") >> get_secondary_timeoff_uri3 >> is_aus_lsl_prorata_accrual_not_present

        is_aus_lsl_prorata_accrual_not_present >> rail.Label("No") >> is_aus_lsl_prorata_accrual_present
        is_aus_lsl_prorata_accrual_not_present >> rail.Label("Yes") >> is_ia_updated3 >> rail.Label("No") >> get_default_timeoff_schedule_policy_for_user4
        is_ia_updated3 >> rail.Label("N0") >> is_ia_equal_1_2 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment3 >> add_dag_run_id_to_wait12 >> empty_policy_updated
        is_ia_equal_1_2 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment3 >> add_dag_run_id_to_wait11 >> empty_policy_updated >> for_each_timeoff_for_location_end

        get_default_timeoff_schedule_policy_for_user4 >> has_any_policy_to_assign4 >> rail.Label("Yes") >> put_user_timeoff_account_policyset_schedule4
        put_user_timeoff_account_policyset_schedule4 >> empty_policy_updated
        has_any_policy_to_assign4 >> rail.Label("No") >> empty_policy_updated

        is_aus_lsl_prorata_accrual_present >> rail.Label("No") >> for_each_timeoff_for_location_end
        is_aus_lsl_prorata_accrual_present >> rail.Label("Yes") >> trigger_aus_lsl_prorata_accrual_timeoff_assignment >> add_dag_run_id_to_wait6 >> for_each_timeoff_for_location_end

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> set_variable_to_store_run_id

        set_variable_to_store_run_id >> date_to_use>> get_all_timeoffs

        get_all_timeoffs >> get_user_timeoff_policy_summary >> get_required_details_for_timeoff_assignment >> is_rehire
        is_rehire >> rail.Label("Yes") >> trigger_rehire_timeoff_assignment >> wait_for_trigger_rehire_timeoff_assignment >> is_fte_updated
        is_rehire >> rail.Label("No") >> is_fte_updated

        is_fte_updated >> rail.Label("Yes") >> for_each_all_assigned_timeoff_data >> for_each_end
        is_fte_updated >> rail.Label("No") >> is_any_timeoff_name_and_uri_present

        for_each_all_assigned_timeoff_data >> get_trigger_id >> is_trigger_id_present >> rail.Label("No") >> for_each_end
        is_trigger_id_present >> rail.Label("Yes") >> trigger_timeoff_assignment >> add_dag_run_id_to_wait4 >> for_each_end
        for_each_end >> is_any_timeoff_name_and_uri_present

        is_any_timeoff_name_and_uri_present >> rail.Label("Yes") >> assign_timeoffs >> for_each_timeoff >> for_each_timeoff_end

        for_each_timeoff >> is_fte_less_than_1 >> rail.Label("Yes") >> is_fte_based_timeoff_calculation_present >> rail.Label(
            "Yes") >> trigger_aus_personal_carers_leave_parttime_child >> add_dag_run_id_to_wait1 >> for_each_timeoff_end
        
        is_fte_based_timeoff_calculation_present >> rail.Label("No") >> is_timeoff_long_service_leave >> rail.Label(
            "Yes") >> is_uri_value_present >> rail.Label("Yes") >> trigger_long_service_leave_timeoff_assignment >> add_dag_run_id_to_wait2 >> for_each_timeoff_end
        is_uri_value_present >> rail.Label("No") >> is_ia_updated >> rail.Label("No") >> get_default_timeoff_schedule_policy_for_user >> has_any_policy_to_assign >> rail.Label(
            "Yes")  >> put_user_timeoff_account_policyset_schedule >> for_each_timeoff_end
        has_any_policy_to_assign >> rail.Label("No") >> for_each_timeoff_end
        
        is_ia_updated >> rail.Label("Yes") >> is_ia_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment >> add_dag_run_id_to_wait7 >> for_each_timeoff_end
        is_ia_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> add_dag_run_id_to_wait8 >> for_each_timeoff_end


        is_timeoff_long_service_leave >> rail.Label("No") >> is_timeoff_name_starts_with_aus_lsl_prorata >> rail.Label(
            "Yes") >> get_secondary_timeoff_uri
        for_each_timeoff_dummy >> for_each_timeoff_end
        
        is_timeoff_name_starts_with_aus_lsl_prorata >> rail.Label("No") >> is_name_starts_with_aus_annual_leave
        for_each_timeoff_end_dummy2 >> for_each_timeoff_end


        is_fte_less_than_1 >> rail.Label("No") >> empty_ftp_not_less_than_1

        empty_ftp_not_less_than_1 >> is_name_starts_with_aus_lsl_and_lsl_timeoff_assigned_and_and_timeoff_name_not_lsl_prorata
        
        is_name_starts_with_aus_lsl_and_lsl_timeoff_assigned_and_and_timeoff_name_not_lsl_prorata >> rail.Label(
            "Yes") >> is_uri_value_present2 >> rail.Label("Yes") >> trigger_long_service_leave_dag >> add_dag_run_id_to_wait3 >> for_each_timeoff_end
        is_uri_value_present2 >> rail.Label("No") >> is_ia_updated_2 >> rail.Label("No") >> get_default_timeoff_schedule_policy_for_user2 >> has_any_policy_to_assign2 >> rail.Label(
            "Yes") >> put_user_timeoff_account_policyset_schedule2 >> for_each_timeoff_end
        is_ia_updated_2 >> rail.Label("yes") >> is_ia_equal_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment2 >> add_dag_run_id_to_wait9 >> for_each_timeoff_end
        is_ia_equal_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment2 >> add_dag_run_id_to_wait10 >> for_each_timeoff_end
        has_any_policy_to_assign2 >> rail.Label("No") >> for_each_timeoff_end 
        is_name_starts_with_aus_lsl_and_lsl_timeoff_assigned_and_and_timeoff_name_not_lsl_prorata >> rail.Label(
            "No") >> is_timeoff_name_starts_with_lsl_prorata >> rail.Label("Yes") >> get_secondary_timeoff_uri2
        for_each_timeoff_dummy2 >> for_each_timeoff_end

        is_timeoff_name_starts_with_lsl_prorata >> rail.Label("No") >> is_name_starts_with_aus_annual_leave2
        for_each_timeoff_end_dummy3 >> for_each_timeoff_end

        for_each_timeoff_end >> is_location_updated
        is_any_timeoff_name_and_uri_present >> rail.Label("No") >> process_timeoff_disable 
        
        process_timeoff_disable >> gather_all_runids_to_wait >> wait_for_dag_run_to_complete >> rail.Label(
            "On Error")>> catch_and_log_error


        return dag

rail.for_each_instance(create_dag)
