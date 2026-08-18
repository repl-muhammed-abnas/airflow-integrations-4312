from datetime import timedelta
from functools import lru_cache
from json import dumps, loads
import json
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_canada_v1.utils.request_payload import get_update_timeoff_policies_payload_update_user, get_timeoff_assignment_payload_for_update_user
from dxctechnology.workday_user_import_v1.user_import_canada.utils.custom_methods import map_mapper_replicon_timeoffs_update_user
from dxctechnology.workday_user_import_v1.user_import_global.utils.custom_methods import get_todays_date_in_json
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import get_date_to_use_for_no_accrual

def create_update_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_canada_users_update_user_timeoff_process_child_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.global_update_user_timeoff_assignment_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_canada, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_all_timeoffs"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_all_timeoffs",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id = "get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        query_timeoff_data = rail.PythonOperator(
            task_id = "query_timeoff_data",
            python_callable=lambda dag_run: list(filter(lambda row: row['Type'] == 'Timeoff' and\
                                                                    row['Country'] == dag_run.conf['country'] and\
                                                                    row['Function'] == 'Workday User Sync' and\
                                                                    row['Source'] == dag_run.conf['parent_company_code'] and\
                                                                    row['personnelsubarea'] == dag_run.conf['personnelsubarea']  and\
                                                                    row['employeegroup'] == dag_run.conf['employeegroup']  and\
                                                                    row['employeesubgroup'] == dag_run.conf['employeesubgroup']  and\
                                                                    row['status'] == dag_run.conf['company_code'] , config.MAPPER))
        )

        map_mapper_replicon_timeoff = rail.PythonOperator(
            task_id = "map_mapper_replicon_timeoff",
            python_callable=map_mapper_replicon_timeoffs_update_user
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda: len(rail.result("map_mapper_replicon_timeoff")) > 0,
            yes_task="get_user_timeoff_policy_summary",
            no_task="stop"
        )

        get_user_timeoff_policy_summary = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri" : "{{dag_run.conf.user_uri}}"
            }
        )

        def get_timeoff_to_disable_and_to_assign_callable():
            current_timeoffs_policies = list(filter(lambda _timeoff: _timeoff['enabled'] in [True, 'true', 'True'] ,map(lambda timeoff : {
                    "name": timeoff['timeOffType']["name"],
                    "enabled": timeoff["isTimeOffAllowedAgainstThisTimeOffType"],
                    "uri":timeoff["timeOffType"]['uri'],
                    "policy":timeoff["policySetSchedule"]
                }, rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'])))

            mapper_timeoff_list = rail.result("map_mapper_replicon_timeoff")
            mapped_timeoff_data = rail.result("map_mapper_replicon_timeoff", "mapped_timeoff_data")

            timeoff_to_assign = list(filter(lambda to1: to1['status']=="No", map(lambda to_uri: {
                "name":rail.find_first_by_attr_and_get_attr(mapped_timeoff_data, 'uri', to_uri, 'name'),
                "enabled":rail.find_first_by_attr_and_get_attr(current_timeoffs_policies, 'uri', to_uri, 'enabled'),
                "uri":to_uri,
                "status": "Yes" if rail.find_first_by_attr_and_get_attr(current_timeoffs_policies, 'uri', to_uri, 'name') else "No"
            }, mapper_timeoff_list)))

            timeoff_to_disable = list(filter(lambda to3: to3['status']=='No', map(lambda to2: {
                "name": to2['name'],
                "enabled": to2['enabled'],
                "uri":to2['uri'],
                "status": "Yes" if rail.find_first_by_attr_and_get_attr(mapper_timeoff_list, 'uri', to2['uri'], 'name') else "No",
                "policy": to2['policy']
            }, current_timeoffs_policies)))

            # RIT-20720: detect targeted disable for non-Yukon Canadians still holding
            # [CAN] Company Holiday. If the user has it but the mapper no longer does,
            # stash its URI so downstream tasks can drop only that one URI from the
            # assign call (preserving manual assignments).
            company_holiday_uri = rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_timeoffs"), 'name', '[CAN] Company Holiday', 'uri', default=None
            )
            company_holiday_uri_to_disable = (
                company_holiday_uri
                if company_holiday_uri
                   and any(p['uri'] == company_holiday_uri for p in current_timeoffs_policies)
                   and company_holiday_uri not in mapper_timeoff_list
                else None
            )

            rail.set_result(key="timeoff_to_disable",val=timeoff_to_disable)
            rail.set_result(key="current_timeoffs_policies", val=current_timeoffs_policies)
            rail.set_result(key="company_holiday_uri_to_disable", val=company_holiday_uri_to_disable)
            return timeoff_to_assign

        get_timeoff_to_disable_and_to_assign = rail.PythonOperator(
            task_id = "get_timeoff_to_disable_and_to_assign",
            python_callable=get_timeoff_to_disable_and_to_assign_callable
        )


        is_rehire_yes_and_parent_company_code_c1 = rail.IfOperator(
            task_id = "is_rehire_yes_and_parent_company_code_c1",
            test = lambda dag_run : dag_run.conf['rehire'] and dag_run.conf['rehire'] == "Yes" and dag_run.conf['parent_company_code'] == "C1",
            yes_task = "trigger_rehire_assignment_for_assigned_timeoff",
            no_task = "is_ia_updated"
        )

        def get_json_conf():
            dag_run_conf = rail.get_dag_run_conf()
            return rail.write_json_artifact(dag_run_conf)

        trigger_rehire_assignment_for_assigned_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_rehire_assignment_for_assigned_timeoff",
            items = lambda : [assigned_timeoff for assigned_timeoff in rail.result(
                                    "get_timeoff_to_disable_and_to_assign", "current_timeoffs_policies") if assigned_timeoff['policy']],
            trigger_dag_id = config.workday_user_import_canada_users_update_user_rehire_timeoff_process_child_dag,
            conf= lambda dag_run, item : {
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "timeoff_type_uri": item['uri'],
                "current_timeoff_policies": item['policy'],
                "timeoff_type_name": item['name'],
                "json_formatted_dates": {
                    "start_date": get_todays_date_in_json(),
                    "continuous_service_date": dag_run.conf['json_formatted_dates']['service_date']
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "other_data": get_json_conf(),
                "fte": dag_run.conf['dag_run_conf']['file_data']['fte'] if dag_run.conf['dag_run_conf']['file_data']['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        wait_for_trigger_rehire_assignment_for_assigned_timeoff = rail.WaitForDagRunsSensor(
            task_id = "wait_for_trigger_rehire_assignment_for_assigned_timeoff",
            dag_runs= "{{ result('trigger_rehire_assignment_for_assigned_timeoff') }}"
        )

        def is_canada_banked_timeoff_assigned()->bool:
            if rail.find_first_by_attr_and_get_attr(
                    rail.result(get_timeoff_to_disable_and_to_assign.task_id),
                    'name',
                    "[CAN] Banked time",
                    default=None
                ):
                 return True
            return False

        def get_secondary_can_banked_timeoff_name(dag_run):
            if dag_run.conf['payrule'] == "Canada Ontario- In/Out":
                    return "[CAN] Banked time - Canada Ontario- In/Out"
            if dag_run.conf['payrule'] == "Canada Quebec- In/Out":
                    return "[CAN] Banked time - Canada Quebec- In/Out"
            return None

        def is_payrule_updated(dag_run):
            if dag_run.conf['payrule_updated']:
                return dag_run.conf['payrule_updated'].lower() == "yes"
            return False

        payrule_updated_and_can_banked_timeoff_not_present = rail.IfOperator(
            task_id = "payrule_updated_and_can_banked_timeoff_not_present",
            test = lambda dag_run: (is_payrule_updated(dag_run) and (not is_canada_banked_timeoff_assigned()) and (bool(get_secondary_can_banked_timeoff_name(dag_run)))),
            yes_task = "trigger_canada_banked_timeoff_assignment",
            no_task = "has_any_timeoff_to_assign_to_user"
        )

        trigger_canada_banked_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_canada_banked_timeoff_assignment",
            trigger_dag_id = config.workday_user_import_canada_users_process_canada_banked_timeoff_type_child_dag,
            items= [1],
            conf=lambda dag_run : {
                "file_name": dag_run.conf['file_name'],
                "emp_id": dag_run.conf['emp_id'],
                "user_uri": dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "login_name": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "country": dag_run.conf['country'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "personnelsubarea": dag_run.conf['personnelsubarea'],
                "employeegroup": dag_run.conf['employeegroup'],
                "employeesubgroup": dag_run.conf['employeesubgroup'],
                "start_date": dag_run.conf['payrule_effective_date'],
                "start_date_json" : dag_run.conf['payrule_effective_date'],
                "timeoff_uri": get_canada_banked_time_timeoff_uri(),
                "timeoff_name": "[CAN] Banked time",
               "secondary_timeoff_policy": rail.find_first_by_attr_and_get_attr(rail.result(get_all_timeoffs.task_id),
                                                                                 'name',
                                                                                 get_secondary_can_banked_timeoff_name(dag_run),
                                                                                 'uri',
                                                                                 default=""),
                "policy_sets": rail.find_first_by_attr_and_get_attr(rail.result(get_user_timeoff_policy_summary.task_id)['policiesByTimeOffType'],
                                                                    'timeOffType.name',
                                                                    '[CAN] Banked time',
                                                                    'policySetSchedule',
                                                                    default=[])
            }
        )

        # RIT-20720: also fire when [CAN] Company Holiday must be disabled for a
        # non-Yukon user with no other adds pending. Payload function picks the right
        # URI list (mapper-eligible vs current-minus-CompanyHoliday) based on case.
        has_any_timeoff_to_assign_to_user = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign_to_user",
            test=lambda: bool(rail.result("get_timeoff_to_disable_and_to_assign"))
                         or bool(rail.result("get_timeoff_to_disable_and_to_assign", "company_holiday_uri_to_disable")),
            yes_task="assign_timeoff_to_user"
        )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=get_timeoff_assignment_payload_for_update_user
        )

        def date_to_use(dag_run, default_return):
            if dag_run.conf['ia_updated'] in ['true', True, 'True']:
                if dag_run.conf['is_ia'] in [1,'1']:
                    return dag_run.conf['ia_start_date']
                return dag_run.conf['ia_end_date']
            return default_return
        # process_normal_timeoffs in for loop
        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items=lambda: [timeoff for timeoff in rail.result("get_timeoff_to_disable_and_to_assign") if timeoff['uri']],
            start_task="is_timeoff_special",
            end_task="empty_process_timeoff"
        )


        def is_timeoff_special_test():
            uri = rail.result("for_each_timeoff")['uri']
            return bool(next(filter(lambda item: item['uri']==uri and item['policy_type'] == "Specific Policy" ,rail.result("map_mapper_replicon_timeoff", "mapped_timeoff_data")), False))

        is_timeoff_special = rail.IfOperator(
            task_id = "is_timeoff_special",
            test = is_timeoff_special_test,
            yes_task = "trigger_vacation_timeoff_assignment_for_users",
            no_task = "payrule_is_required_for_canada_banked_time"
        )

        trigger_vacation_timeoff_assignment_for_users = rail.TriggerDagRunOperator(
            task_id = "trigger_vacation_timeoff_assignment_for_users",
            trigger_dag_id=config.workday_user_import_canada_users_process_canada_vacation_timeoff_type_child_dag,
            conf=lambda dag_run: {
                "file_name": dag_run.conf['file_name'],
                "emp_id": dag_run.conf['emp_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "payrule":  dag_run.conf['payrule'],
                "company_code": dag_run.conf['company_code'],
                "parent_company_code": dag_run.conf['parent_company_code'],
                "country": dag_run.conf['country'],
                'user_log': dag_run.conf['user_log'],
                "personnelsubarea": dag_run.conf['personnelsubarea'],
                "employeegroup": dag_run.conf['employeegroup'],
                "employeesubgroup": dag_run.conf['employeesubgroup'],
                "status": dag_run.conf['company_code'],
                "json_formatted_dates": {
                    "continuous_service_date": dag_run.conf['continuous_service_date'],
                    "start_date": date_to_use(dag_run, dag_run.conf['start_date'])
                },
                "file_data": {
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id']
                },
                "timeoff_type_uri": rail.result("for_each_timeoff")["uri"],
                "timeoff_type_name": rail.result("for_each_timeoff")["name"]
            },
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        def payrule_is_required_for_canada_banked_time_callable(dag_run):
            if rail.result("for_each_timeoff")['name'] == "[CAN] Banked time":
                return get_secondary_can_banked_timeoff_name(dag_run)
            return None

        payrule_is_required_for_canada_banked_time = rail.PythonOperator(
            task_id = "payrule_is_required_for_canada_banked_time",
            python_callable = payrule_is_required_for_canada_banked_time_callable
        )

        is_payrule_is_required_for_canada_banked_time_present = rail.IfOperator(
            task_id = "is_payrule_is_required_for_canada_banked_time_present",
            test = "{{result('payrule_is_required_for_canada_banked_time') | is_truthy}}",
            yes_task = "is_canada_banked_time_not_assigned",
            no_task = "is_ia_updated_3"
        )

        is_canada_banked_time_not_assigned = rail.IfOperator(
            task_id = "is_canada_banked_time_not_assigned",
            test = lambda: not bool(rail.find_first_by_attr_and_get_attr(
                    rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'],
                    "timeOffType.name",
                    "[CAN] Banked time"
                )),
            yes_task = "get_canada_banked_time_secondary_uri",
            no_task = "trigger_canada_banked_time_assignment_for_existing_users"
        )

        get_canada_banked_time_secondary_uri = rail.PythonOperator(
            task_id = "get_canada_banked_time_secondary_uri",
            python_callable = lambda : rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), 'name', rail.result("payrule_is_required_for_canada_banked_time"))
        )

        is_ia_updated_2 = rail.IfOperator(
            task_id = "is_ia_updated_2",
            test = lambda dag_run: dag_run.conf['ia_updated'] in [True, 'true', 'True'],
            yes_task = "trigger_canada_banked_time_assignment_for_existing_users2",
            no_task = "get_default_timeoff_policy2"
        )

        trigger_canada_banked_time_assignment_for_existing_users2 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_canada_banked_time_assignment_for_existing_users2",
            trigger_dag_id = config.workday_user_import_canada_users_process_canada_banked_timeoff_type_child_dag,
            items= [1],
            conf=lambda dag_run : {
                "file_name": dag_run.conf['file_name'],
                "emp_id": dag_run.conf['emp_id'],
                "user_uri": dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "login_name": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "country": dag_run.conf['country'],
                "personnelsubarea": dag_run.conf['personnelsubarea'],
                "employeegroup": dag_run.conf['employeegroup'],
                "employeesubgroup": dag_run.conf['employeesubgroup'],
                "start_date": dag_run.conf['payrule_effective_date'],
                "start_date_json" :date_to_use(dag_run, dag_run.conf['json_formatted_dates']['payrule_effective_date']),
                "timeoff_uri": get_canada_banked_time_timeoff_uri(),
                "timeoff_name": "[CAN] Banked time",
                "secondary_timeoff_policy": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), 'name', rail.result("payrule_is_required_for_canada_banked_time")),
                "policy_sets": []
                
            }
        )


        get_default_timeoff_policy2 = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy2",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                }
            }
        )

        has_any_policies2 = rail.IfOperator(
            task_id = "has_any_policies2",
            test=lambda : bool(rail.result("get_default_timeoff_policy2")[0]['policySet'] if rail.result("get_default_timeoff_policy2") else []),
            yes_task="update_timeoff_policies2",
            no_task="empty_process_timeoff"
        )

        def get_update_timeoff_policies_payload_update_user2(dag_run):
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                },
                "policySetScheduleEntries": loads(dumps(rail.result("get_default_timeoff_policy2")
                            ).replace("null", "\"effective\""
                        ).replace("\"script\"", "\"scriptTarget\""
                        ))
            }

        update_timeoff_policies2 = rail.RepliconServiceOperator(
            task_id = "update_timeoff_policies2",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_update_timeoff_policies_payload_update_user2
        )

        @lru_cache(maxsize=8)
        def get_canada_banked_time_timeoff_uri():
            return rail.find_first_by_attr_and_get_attr(rail.result(get_all_timeoffs.task_id),
                                                        'name',
                                                        "[CAN] Banked time",
                                                        "uri",
                                                        default="")

        trigger_canada_banked_time_assignment_for_existing_users = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_canada_banked_time_assignment_for_existing_users",
            trigger_dag_id = config.workday_user_import_canada_users_process_canada_banked_timeoff_type_child_dag,
            items= [1],
            conf=lambda dag_run : {
                "file_name": dag_run.conf['file_name'],
                "emp_id": dag_run.conf['emp_id'],
                "user_uri": dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "login_name": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "country": dag_run.conf['country'],
                "personnelsubarea": dag_run.conf['personnelsubarea'],
                "employeegroup": dag_run.conf['employeegroup'],
                "employeesubgroup": dag_run.conf['employeesubgroup'],
                "start_date": dag_run.conf['payrule_effective_date'],
                "start_date_json" : dag_run.conf['payrule_effective_date'],
                "timeoff_uri": get_canada_banked_time_timeoff_uri(),
                "timeoff_name": "[CAN] Banked time",
                "secondary_timeoff_policy": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), 'name', rail.result("payrule_is_required_for_canada_banked_time")),
                "policy_sets": loads(dumps(rail.find_first_by_attr_and_get_attr(rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'], 'timeOffType.name' , '[CAN] Banked time', 'policySetSchedule', default=[])).replace("[[{", "[{").replace("}]]", "}]"))
                
            }
        )

        is_ia_updated_3 = rail.IfOperator(
            task_id = "is_ia_updated_3",
            test = lambda dag_run: dag_run.conf['ia_updated'] in [True, 'true', 'True'],
            yes_task = "is_ia_1",
            no_task = "get_default_timeoff_policy"
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
                "star_date": dag_run.conf['ia_start_date'],
                "country": dag_run.conf['country'],
                "personnel_subarea": "",
                "employee_group":"",
                "employee_subgroup": "",
                "contineous_service_date": dag_run.conf['json_formatted_dates']['hire_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "secondary_timeoff_uri": None,
                "policy": [],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['ia_start_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
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
                "star_date": dag_run.conf['json_formatted_dates']['hire_date'],
                "ia_end_date": dag_run.conf['ia_end_date'],
                "country": dag_run.conf['country'],
                "personnel_subarea": "",
                "employee_group":"",
                "employee_subgroup": "",
                "contineous_service_date": dag_run.conf['json_formatted_dates']['hire_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "secondary_timeoff_uri": None,
                "policy": [],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "ia_end_date": dag_run.conf['ia_end_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        get_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                }
            }
        )

        has_any_policies = rail.IfOperator(
            task_id = "has_any_policies",
            test=lambda : bool(rail.result("get_default_timeoff_policy")[0]['policySet'] if rail.result("get_default_timeoff_policy") else []),
            yes_task="update_timeoff_policies",
            no_task="empty_process_timeoff"
        )

        update_timeoff_policies = rail.RepliconServiceOperator(
            task_id = "update_timeoff_policies",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_update_timeoff_policies_payload_update_user
        )

        is_ia_updated = rail.IfOperator(
            task_id = "is_ia_updated",
            test = lambda dag_run: dag_run.conf['ia_updated'] in [True, 'true', 'True'],
            yes_task = "process_no_accrual",
            no_task = "payrule_updated_and_can_banked_timeoff_not_present"
        )        

        process_no_accrual = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_no_accrual",
            items=lambda: [timeoff for timeoff in rail.result(
                    "get_timeoff_to_disable_and_to_assign", "timeoff_to_disable") if timeoff['policy']],
                trigger_dag_id=config.process_time_off_accrual,
                conf=lambda dag_run, item: {
                    **{
                        "file_name": dag_run.conf["file_name"],
                        "user_uri": dag_run.conf["user_uri"],
                        "user_log": dag_run.conf['user_log'],
                        "emp_id": dag_run.conf['emp_id'],
                        "email_id": dag_run.conf["email_id"],
                        "loginName": dag_run.conf['loginName'],
                        "start_date": {},
                        "hire_date": dag_run.conf['json_formatted_dates']['hire_date'],
                        "end_date": get_date_to_use_for_no_accrual(dag_run, return_type='str'), # default return is not added as this will executed only when the IA is updated,
                        "end_date_json": get_date_to_use_for_no_accrual(dag_run),
                        "company_code": dag_run.conf['company_code'],
                        "parent_company_code": dag_run.conf['parent_company_code'],
                        "country": dag_run.conf['country'],
                        "timeoffs": dag_run.conf['timeoffs'],
                        "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                        "prevent_balance_overdraw_uri": dag_run.conf["prevent_balance_overdraw_uri"],
                        "parent_location" : dag_run.conf['parent_location'],
                        "is_ia": dag_run.conf['is_ia'],
                        "ia_updated":  dag_run.conf["ia_updated"],
                        "ia_end_date": dag_run.conf['ia_end_date'],
                        "ia_start_date": dag_run.conf['ia_start_date'],
                        "assignment_type": dag_run.conf['assignment_type']
                    },
                    **{
                        "timeoff_type_uri": item['uri'],
                        "policy_set": dumps(item['policy']).replace("[[{", "[{").replace("}]]", "}]"),
                        "user_end_date_json": get_date_to_use_for_no_accrual(dag_run)
                    }
                },
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
        )

        wait_for_no_accrual = rail.WaitForDagRunsSensor(
            task_id = "wait_for_no_accrual",
            dag_runs="{{result('process_no_accrual')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
        )

        stop = rail.EmptyOperator(
             task_id = "stop"
        )

        empty_process_timeoff = rail.EmptyOperator(
            task_id = "empty_process_timeoff"
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Update",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Update',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_all_timeoffs

        has_any_timeoff_to_assign >> rail.Label("No") >> stop

        get_all_timeoffs >> query_timeoff_data >> map_mapper_replicon_timeoff >> has_any_timeoff_to_assign >> rail.Label(
            "Yes") >> get_user_timeoff_policy_summary >> get_timeoff_to_disable_and_to_assign >> is_rehire_yes_and_parent_company_code_c1 >> rail.Label("No") >> is_ia_updated
        payrule_updated_and_can_banked_timeoff_not_present >> trigger_canada_banked_timeoff_assignment >> has_any_timeoff_to_assign_to_user
        payrule_updated_and_can_banked_timeoff_not_present >> rail.Label("No") >> has_any_timeoff_to_assign_to_user >> rail.Label(
            "Yes") >> assign_timeoff_to_user >> for_each_timeoff

        is_ia_updated >> rail.Label("Yes") >> process_no_accrual >> wait_for_no_accrual >> payrule_updated_and_can_banked_timeoff_not_present
        is_ia_updated >> rail.Label("No") >> payrule_updated_and_can_banked_timeoff_not_present
        
        is_rehire_yes_and_parent_company_code_c1 >> rail.Label("Yes") >> trigger_rehire_assignment_for_assigned_timeoff >> wait_for_trigger_rehire_assignment_for_assigned_timeoff >> is_ia_updated

        for_each_timeoff >> is_timeoff_special >> rail.Label("Yes") >> trigger_vacation_timeoff_assignment_for_users >> empty_process_timeoff
        is_timeoff_special >> rail.Label("No") >> payrule_is_required_for_canada_banked_time >> is_payrule_is_required_for_canada_banked_time_present >> rail.Label("Yes") >> is_canada_banked_time_not_assigned
        is_canada_banked_time_not_assigned >> rail.Label("Yes") >> get_canada_banked_time_secondary_uri >> is_ia_updated_2 >> rail.Label("No")>> get_default_timeoff_policy2 >> has_any_policies2 >> rail.Label("Yes") >> update_timeoff_policies2 >> empty_process_timeoff
        has_any_policies2 >> rail.Label("No") >> empty_process_timeoff
        is_ia_updated_2 >> rail.Label("Yes") >> trigger_canada_banked_time_assignment_for_existing_users2 >> empty_process_timeoff

        is_canada_banked_time_not_assigned >> rail.Label("No") >> trigger_canada_banked_time_assignment_for_existing_users >> empty_process_timeoff

        is_ia_updated_3 >> is_ia_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment >> empty_process_timeoff
        is_ia_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> empty_process_timeoff
        is_payrule_is_required_for_canada_banked_time_present >> rail.Label("No") >> is_ia_updated_3 >> rail.Label("No") >> get_default_timeoff_policy >> has_any_policies >> rail.Label("Yes") >> update_timeoff_policies >> empty_process_timeoff
        has_any_policies >> rail.Label("Yes") >> empty_process_timeoff >> stop >> catch_and_log_error
        for_each_timeoff >> empty_process_timeoff

        return dag

rail.for_each_instance(create_update_user_timeoff_assignment_dag)
