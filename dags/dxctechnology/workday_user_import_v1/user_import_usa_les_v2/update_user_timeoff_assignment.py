from datetime import timedelta
from json import dumps, loads
from numpy import empty
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import convert_json_date_to_date, get_json_date_from_date, get_replicon_date,get_date_to_use_for_no_accrual, convert_json_date_to_string_date
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_json_date_from_date_str, get_todays_date_in_json

def create_update_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.usa_lse_update_user_timeoff_assignment_dag_id,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=10
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_usa_les, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="date_to_considered"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="date_to_considered",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        def date_to_considered_callable(dag_run):
            return dag_run.conf['start_date']

        date_to_considered = rail.PythonOperator(
            task_id = "date_to_considered",
            python_callable = date_to_considered_callable
        )

        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id = "get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        def get_employeesubgroup_to_use(dag_run):
            if dag_run.conf['employeetype'] == "Non Exempt - Hourly":
                if dag_run.conf['paygroup']:
                    if dag_run.conf['paygroup'].lower() == "usa-bi-weekly":
                        return "USA-Bi-Weekly"
            return "All Others"

        query_timeoff_data = rail.PythonOperator(
            task_id = "query_timeoff_data",
            python_callable=lambda dag_run: list(filter(lambda row: row['Type']=='Timeoff' and\
                                                                    row['Country']==dag_run.conf['country'] and\
                                                                    row['Function']=='Workday User Sync' and\
                                                                    row['personnelsubarea']==dag_run.conf['employeetype'] and\
                                                                    row['employeesubgroup']==get_employeesubgroup_to_use(dag_run) and\
                                                                    row['Source'] == dag_run.conf['parent_company_code'], config.MAPPER))
        )

        has_any_data_found = rail.IfOperator(
            task_id = "has_any_data_found",
            test=lambda: bool(rail.result("query_timeoff_data")),
            yes_task="get_required_details",
            no_task="dummy_no_task_has_any_data_found"
        )

        dummy_no_task_has_any_data_found = rail.EmptyOperator(
            task_id = "dummy_no_task_has_any_data_found"
        )

        empty_gather_all_triggered_dag_runs = rail.EmptyOperator(
            task_id = "empty_gather_all_triggered_dag_runs"
        )

        def get_required_details_callable(dag_run):
            mapper_to_data = rail.result("query_timeoff_data")
            replicon_to_data = rail.result("get_all_timeoffs")

            return_data =  list(map(lambda item: {
                "name": item['Value'],
                "uri": rail.find_first_by_attr_and_get_attr(replicon_to_data, 'name', item['Value'].strip(), 'uri', default="")
            }, mapper_to_data))

            return {
                "return_data" : return_data,
                "unique_uri_data" : list(set(i['uri'] for i in return_data if i['uri'])),
                "unique_uri_data_len" : len(set(i['uri'] for i in return_data if i['uri']))
            }

        get_required_details = rail.PythonOperator(
            task_id = "get_required_details",
            python_callable=get_required_details_callable
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda : rail.result("get_required_details")['unique_uri_data_len'] > 0,
            yes_task="get_user_timeoff_policy_summary",
            no_task="dummy_no_task_has_any_timeoff_to_assign"
        )

        dummy_no_task_has_any_timeoff_to_assign = rail.EmptyOperator(
            task_id = "dummy_no_task_has_any_timeoff_to_assign"
        )

        get_user_timeoff_policy_summary = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri" : "{{dag_run.conf.user_uri}}"
            }
        )

        def get_required_details_2_callable(dag_run):

            user_timeoff_policy_summary = rail.result("get_user_timeoff_policy_summary")

            assignedtimeofftypes = list(filter(lambda x:x['enabled'] in ['true', True, 'True'], map(lambda item: {
                                            "name": item['timeOffType']['displayText'],
                                            "enabled":item['isTimeOffAllowedAgainstThisTimeOffType'],
                                            "uri": item['timeOffType']['uri'],
                                            "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
                                        }, user_timeoff_policy_summary['policiesByTimeOffType'])))
            raw_finaltimeofflist = rail.result("get_required_details")
            finaltimeofflist = [i["uri"] for i in raw_finaltimeofflist['return_data']]
            timeofftypestobeassigned = list(filter(lambda item2: item2['status'] == "No",map(lambda item: {
                                            "name": item['name'],
                                            "enabled": rail.find_first_by_attr_and_get_attr(assignedtimeofftypes, 'uri', item['uri'], 'enabled'),
                                            "uri": item['uri'],
                                            "status": "Yes" if rail.find_first_by_attr_and_get_attr(assignedtimeofftypes, 'name', item['name'], 'enabled', default=False) is True else "No" 
                                        }, raw_finaltimeofflist['return_data'])))
            timeofftypestobedisabled = list(filter(lambda item: item['status']== "No", map(lambda item2: {
                    **item2,
                    **{
                     "status": "Yes" if bool(rail.find_first_by_attr_and_get_attr(raw_finaltimeofflist['return_data'], 'uri', item2['uri'], 'name', default="")) else "No"
                    }
            }, assignedtimeofftypes)))
 
            return {
                "assignedtimeofftypes": assignedtimeofftypes,
                "finaltimeofflist": finaltimeofflist,
                "timeofftypestobeassigned": [_toa for _toa in timeofftypestobeassigned if _toa['uri']],
                "timeofftypestobedisabled": timeofftypestobedisabled
            }

        get_required_details_2 = rail.PythonOperator(
            task_id = "get_required_details_2",
            python_callable=get_required_details_2_callable
        )

        set_variable_to_store_run_id = rail.GetVariableOperator(
            task_id = "set_variable_to_store_run_id",
            name="variable_to_store_run_id"
        )

        def is_timeoff_not_holiday_sick_leave_test(task_id):
            return rail.result(task_id)['name'] not in ['[USA] ES Holiday', '[USA] ES Sick Time']

        is_rehire_true = rail.IfOperator(
            task_id = "is_rehire_true",
            test="{{dag_run.conf.rehire == 'Yes'}}",
            yes_task="process_each_assigned_timeoffs",
            no_task="process_no_accrual"
        )

        process_each_assigned_timeoffs = rail.ForEachOperator(
            task_id = "process_each_assigned_timeoffs",
            items=lambda : [timeoff for timeoff in rail.result("get_required_details_2")['assignedtimeofftypes'] if timeoff['policy']],
            start_task="is_timeoff_not_holiday_sick_leave",
            end_task="end_process_each_assigned_timeoffs"
        )

        is_timeoff_not_holiday_sick_leave = rail.IfOperator(
            task_id = "is_timeoff_not_holiday_sick_leave",
            test = lambda: is_timeoff_not_holiday_sick_leave_test(process_each_assigned_timeoffs.task_id),
            yes_task = "trigger_rehire_timeoff_assignment",
            no_task="is_timeoff_name_usa_holiday"
        )

        def get_json_conf():
            dag_run_conf = rail.get_dag_run_conf()
            return rail.write_json_artifact(dag_run_conf)


        trigger_rehire_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_rehire_timeoff_assignment",
            items=[1],
            trigger_dag_id = config.workday_user_import_usa_les_users_update_user_rehire_timeoff_process_child_dag,
            conf= lambda dag_run, item : {
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "timeoff_type_uri": rail.result('process_each_assigned_timeoffs')['uri'],
                "current_timeoff_policies": rail.result('process_each_assigned_timeoffs')['policy'],
                "timeoff_type_name": rail.result('process_each_assigned_timeoffs')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "continuous_service_date": dag_run.conf['json_formatted_dates']['service_date']
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "other_data": get_json_conf(),
                "fte": dag_run.conf['fte'] if dag_run.conf['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_dag_run_id_to_wait1 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait1",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_rehire_timeoff_assignment"),
            append=True
        )

        is_timeoff_name_usa_holiday = rail.IfOperator(
            task_id = "is_timeoff_name_usa_holiday",
            test = lambda: rail.result("process_each_assigned_timeoffs")['name'] == "[USA] ES Holiday",
            yes_task="process_rehire_usa_holiday",
            no_task="is_timeoff_name_us_sick_time"
        )

        process_rehire_usa_holiday = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_rehire_usa_holiday",
            items = [1],
            trigger_dag_id = config.usa_les_us_holiday_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['json_formatted_dates']['service_date'],
                "timeoff_uri": rail.result('process_each_assigned_timeoffs')['uri'],
                "timeoff_name": rail.result('process_each_assigned_timeoffs')['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Updated",
                "policy_sets": rail.result('process_each_assigned_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['start_date'],
                "fte": dag_run.conf['fte']
            }
        )

        add_dag_run_id_to_wait2 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait2",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("process_rehire_usa_holiday"),
            append=True
        )

        is_timeoff_name_us_sick_time = rail.IfOperator(
            task_id = "is_timeoff_name_us_sick_time",
            test=lambda: rail.result("process_each_assigned_timeoffs")['name'] == "[USA] ES Sick Time",
            yes_task="trigger_sick_non_california_timeoff_dag",
            no_task="end_process_each_assigned_timeoffs"
        )

        trigger_sick_non_california_timeoff_dag = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_sick_non_california_timeoff_dag",
            items = [1],
            trigger_dag_id = config.usa_les_us_sick_leave_non_california_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['json_formatted_dates']['service_date'],
                "timeoff_uri": rail.result("process_each_assigned_timeoffs")['uri'],
                "timeoff_name": rail.result("process_each_assigned_timeoffs")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Update",
                "policy_sets": rail.result('process_each_assigned_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['start_date'],
                "fte": dag_run.conf['fte'],
                "fte_updated": dag_run.conf['fte_updated'],
                "effective_date_to_use_for_sick_timeoff": dag_run.conf['start_date']
            }
        )

        add_dag_run_id_to_wait4 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait4",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_sick_non_california_timeoff_dag"),
            append=True
        )

        end_process_each_assigned_timeoffs = rail.EmptyOperator(
            task_id = "end_process_each_assigned_timeoffs"
        )
        
        def is_schedule_changed_and_not_rehire_not_ia_updated_test(dag_run):
            if (dag_run.conf['schedule_change'] and dag_run.conf['schedule_change'].lower() == "yes") and (not dag_run.conf['rehire']) and (dag_run.conf['is_ia_updated'].lower() != "yes"):\
                return True
            return False

        is_schedule_changed_and_not_rehire_not_ia_updated = rail.IfOperator(
            task_id = "is_schedule_changed_and_not_rehire_not_ia_updated",
            test=is_schedule_changed_and_not_rehire_not_ia_updated_test,
            yes_task="process_usa_holiday_and_sick_timeoffs",
            no_task="has_any_timeoff_to_be_assign"
        )

        process_usa_holiday_and_sick_timeoffs = rail.ForEachOperator(
            task_id = "process_usa_holiday_and_sick_timeoffs",
            items=lambda : rail.result("get_required_details_2")['assignedtimeofftypes'],
            start_task="can_process_timeoff",
            end_task="end_process_each_assigned_timeoffs2"
        )

        def can_process_timeoff_test(dag_run):
            timeoff_assignment_details = rail.result("get_required_details_2")
            timeoff_tobe_assigned = timeoff_assignment_details['timeofftypestobeassigned']
            timeoff_tobe_disabled = timeoff_assignment_details['timeofftypestobedisabled']

            processing_to_name = rail.result("process_usa_holiday_and_sick_timeoffs")['name']

            if processing_to_name in ['[USA] ES Holiday', '[USA] ES Sick Time']:
                
                if not bool(rail.find_first_by_attr_and_get_attr(timeoff_tobe_disabled, 'name', processing_to_name)):
                    if not bool(rail.find_first_by_attr_and_get_attr(timeoff_tobe_assigned, 'name', processing_to_name)):
                        return True


            return False


        can_process_timeoff = rail.IfOperator(
            task_id = "can_process_timeoff",
            test = can_process_timeoff_test,
            yes_task="is_timeoff_name_usa_holiday2_dummy",
            no_task="end_process_each_assigned_timeoffs2"
        )

        is_timeoff_name_usa_holiday2_dummy = rail.EmptyOperator(
            task_id = "is_timeoff_name_usa_holiday2_dummy"
        )

        is_timeoff_name_usa_holiday2 = rail.IfOperator(
            task_id = "is_timeoff_name_usa_holiday2",
            test = lambda: rail.result("process_usa_holiday_and_sick_timeoffs")['name'] == "[USA] ES Holiday",
            yes_task="process_usa_holiday_timeoff",
            no_task="is_timeoff_name_us_sick_time2"
        )

        process_usa_holiday_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_usa_holiday_timeoff",
            items = [1],
            trigger_dag_id = config.usa_les_us_holiday_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['json_formatted_dates']['service_date'],
                "timeoff_uri": rail.result("process_usa_holiday_and_sick_timeoffs")['uri'],
                "timeoff_name": rail.result("process_usa_holiday_and_sick_timeoffs")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Update",
                "policy_sets": rail.result("process_usa_holiday_and_sick_timeoffs")['policy'],
                "schedule_changed_date": dag_run.conf['schedule_changed_date'],
                "fte": dag_run.conf['fte']
            }
        )

        add_dag_run_id_to_wait5 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait5",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("process_usa_holiday_timeoff"),
            append=True
        )

        is_timeoff_name_us_sick_time2 = rail.IfOperator(
            task_id = "is_timeoff_name_us_sick_time2",
            test=lambda: rail.result("process_usa_holiday_and_sick_timeoffs")['name'] == "[USA] ES Sick Time",
            yes_task="trigger_sick_non_california_timeoff_dag2",
            no_task="end_process_each_assigned_timeoffs2"
        )

        trigger_sick_non_california_timeoff_dag2 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_sick_non_california_timeoff_dag2",
            items = [1],
            trigger_dag_id = config.usa_les_us_sick_leave_non_california_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['json_formatted_dates']['service_date'],
                "timeoff_uri": rail.result("process_usa_holiday_and_sick_timeoffs")['uri'],
                "timeoff_name": rail.result("process_usa_holiday_and_sick_timeoffs")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Update",
                "policy_sets": rail.result('process_usa_holiday_and_sick_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['schedule_changed_date'],
                "fte": dag_run.conf['fte'],
                "fte_updated": dag_run.conf['fte_updated'],
                "effective_date_to_use_for_sick_timeoff": get_todays_date_in_json() if dag_run.conf['schedule_change'] == "Yes" else dag_run.conf['schedule_changed_date']
            }
        )

        add_dag_run_id_to_wait7 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait7",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_sick_non_california_timeoff_dag2"),
            append=True
        )

        end_process_each_assigned_timeoffs2 = rail.EmptyOperator(
            task_id = "end_process_each_assigned_timeoffs2"
        )

        def no_accrual_test(dag_run, item):
            if dag_run.conf['is_ia_updated'].lower() == "yes":
                if item['name'] != "[USA] ES Sick Time":
                    return True
            return False

        process_no_accrual = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_no_accrual",
            items=lambda dag_run: [timeoff for timeoff in rail.result(
                    "get_required_details_2")['timeofftypestobedisabled'] if no_accrual_test(dag_run, timeoff)],
            trigger_dag_id=config.process_time_off_accrual,
            conf=lambda dag_run, item: {
                **dag_run.conf,
                **{
                    "timeoff_type_uri": item['uri'],
                    "policy_set": dumps(item['policy']).replace("[[{", "[{").replace("}]]", "}]"),
                    "user_end_date_json": get_date_to_use_for_no_accrual(dag_run),
                    "user_log": dag_run.conf['user_log'],
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        process_no_accrual_sick_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_no_accrual_sick_timeoff",
            items=lambda: [timeoff for timeoff in rail.result(
                    "get_required_details_2")['timeofftypestobedisabled'] if timeoff['policy'] and timeoff['name']=="[USA] ES Sick Time"],
            trigger_dag_id=config.process_time_off_accrual,
            conf=lambda dag_run, item: {
                **dag_run.conf,
                **{
                    "timeoff_type_uri": item['uri'],
                    "policy_set": dumps(item['policy']).replace("[[{", "[{").replace("}]]", "}]"),
                    "user_end_date_json": get_date_to_use_for_no_accrual(dag_run),
                    "user_log": dag_run.conf['user_log'],
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

        has_any_timeoff_to_be_assign  = rail.IfOperator(
            task_id = "has_any_timeoff_to_be_assign",
            test = lambda : bool(rail.result("get_required_details_2")["timeofftypestobeassigned"]),
            yes_task = "assign_timeoff_to_user",
            no_task = "dummy_no_task_has_any_timeoff_to_be_assign"
        )

        dummy_no_task_has_any_timeoff_to_be_assign = rail.EmptyOperator(
            task_id = "dummy_no_task_has_any_timeoff_to_be_assign"
        )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run :{
                "userUri": dag_run.conf['user_uri'],
                "timeOffTypeUris": [uri for uri in rail.result("get_required_details")['unique_uri_data'] if uri]
            }
        )

        def get_date_to_use(dag_run):
            if dag_run.conf['is_ia_updated'] == "Yes":
                if dag_run.conf['is_ia'] == "1":
                    if "host pay" in dag_run.conf['assignment_type']:
                        return dag_run.conf['ia_start_date']
                if dag_run.conf['is_ia'] == "0":
                    return get_json_date_from_date(convert_json_date_to_date(dag_run.conf['ia_end_date']) + timedelta(days=1))
            return None

        date_to_use = rail.PythonOperator(
            task_id = "date_to_use",
            python_callable=get_date_to_use
        )

        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items = lambda: rail.result("get_required_details_2")["timeofftypestobeassigned"],
            start_task = "is_timeoff_name_not_us_holiday_us_sick_leave",
            end_task = "for_each_end"
        )

        is_timeoff_name_not_us_holiday_us_sick_leave = rail.IfOperator(
            task_id = "is_timeoff_name_not_us_holiday_us_sick_leave",
            test=lambda: is_timeoff_not_holiday_sick_leave_test("for_each_timeoff"),
            yes_task = "is_ia_updated_1",
            no_task = "is_timeoff_usa_sick_time"
        )

        is_timeoff_usa_sick_time = rail.IfOperator(
            task_id = "is_timeoff_usa_sick_time",
            test=lambda: rail.result("for_each_timeoff")['name'] == "[USA] ES Sick Time",
            yes_task="trigger_sick_non_california_timeoff",
            no_task="empty_is_timeoff_usa_holiday"
        )

        empty_is_timeoff_usa_holiday = rail.EmptyOperator(
            task_id = "empty_is_timeoff_usa_holiday"
        )

        is_timeoff_usa_holiday = rail.IfOperator(
            task_id = "is_timeoff_usa_holiday",
            test = lambda: rail.result("for_each_timeoff")['name'] == "[USA] ES Holiday",
            yes_task = "trigger_usa_holiday_timeoff_child"
        )

        def get_effective_date(dag_run, default_date, return_type="json", validate_schedule_change=True):
            
            if dag_run.conf['is_ia_updated'].lower() == "yes":
                if dag_run.conf['is_ia'] in ['1',1]:
                    if "host pay" in dag_run.conf['assignment_type'].lower():
                        if return_type == "json":
                            ia_start_date = dag_run.conf['ia_start_date']
                            return ia_start_date if isinstance(ia_start_date, dict) else get_json_date_from_date_str(ia_start_date)
                        return dag_run.conf['ia_start_date']
                if dag_run.conf['is_ia'] in ['0',0]:
                    ia_end_date = dag_run.conf['ia_end_date']
                    _end_date = (convert_json_date_to_date(ia_end_date) if isinstance(ia_end_date, dict) else get_replicon_date(ia_end_date)) + timedelta(days=1)
                    if return_type == "json":
                        return get_json_date_from_date(_end_date)
                    return _end_date.strftime("%Y-%d-%m")

            if validate_schedule_change and dag_run.conf['schedule_change'] and dag_run.conf['schedule_change'].lower() == "yes":
                if return_type == "json":
                    return get_todays_date_in_json()
                return convert_json_date_to_string_date(get_todays_date_in_json())

            if return_type == "json":
                if not default_date:
                    return None
                return default_date if isinstance(default_date, dict) else get_json_date_from_date_str(default_date)
            return default_date

        def get_caller(dag_run):
            if dag_run.conf['is_ia_updated'].lower() == "yes":
                if dag_run.conf['is_ia'] in ['1',1]:
                    if "host pay" in dag_run.conf['assignment_type'].lower():
                        return "Update"
                if dag_run.conf['is_ia'] in ['0',0]:
                    return "Update"
            return "Add"


        trigger_usa_holiday_timeoff_child = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_usa_holiday_timeoff_child",
            items = [1],
            trigger_dag_id = config.usa_les_us_holiday_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['json_formatted_dates']['service_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": get_caller(dag_run),
                "policy_sets": [] if get_caller(dag_run) == "Add" else rail.find_first_by_attr_and_get_attr(rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'], 'timeOffType', 'uri', rail.result("for_each_timeoff")['uri'], 'policySetSchedule', default=[]),
                "schedule_changed_date": get_effective_date(dag_run, None, return_type="json", validate_schedule_change=False),
                "fte": dag_run.conf['fte']
            }
        )

        add_dag_run_id_to_wait12 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait12",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_usa_holiday_timeoff_child"),
            append=True
        )

        trigger_sick_non_california_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_sick_non_california_timeoff",
            items = [1],
            trigger_dag_id = config.usa_les_us_sick_leave_non_california_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['json_formatted_dates']['service_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": get_caller(dag_run),
                "policy_sets": [] if get_caller(dag_run) == "Add" else rail.find_first_by_attr_and_get_attr(rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'], 'timeOffType', 'uri', rail.result("for_each_timeoff")['uri'], 'policySetSchedule', default=[]),
                "schedule_changed_date": get_effective_date(dag_run, dag_run.conf['start_date'], return_type="str"),
                "fte": dag_run.conf['fte'],
                "fte_updated": dag_run.conf['fte_updated'],
                "effective_date_to_use_for_sick_timeoff": get_effective_date(dag_run, dag_run.conf['start_date'])
            }
        )

        add_dag_run_id_to_wait11 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait11",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_sick_non_california_timeoff"),
            append=True
        )

        is_ia_updated_1 = rail.IfOperator(
            task_id = "is_ia_updated_1",
            test= lambda dag_run: dag_run.conf['is_ia_updated'].lower() == "yes",
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
                "user_log": dag_run.conf['user_log'],
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
                "secondary_timeoff_uri": None,
                "policy": [],
                "json_formatted_dates": {
                    "start_date": get_json_date_from_date_str(dag_run.conf['ia_start_date'])
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        add_dag_run_id_to_wait8 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait8",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_one_timeoff_assignment"),
            append=True
        )

        trigger_ia_zero_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_ia_zero_timeoff_assignment",
            items=[1],
            trigger_dag_id=config.workday_user_import_ia_zero_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
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
                "secondary_timeoff_uri": None,
                "policy": [],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['hire_date'],
                    "ia_end_date": dag_run.conf['ia_end_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        add_dag_run_id_to_wait9 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait9",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_zero_timeoff_assignment"),
            append=True
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
            no_task="for_each_end"
        )

        update_timeoff_policies = rail.RepliconServiceOperator(
            task_id = "update_timeoff_policies",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                },
                "policySetScheduleEntries": loads(dumps(rail.result("get_default_timeoff_policy")
                            ).replace("null", "\"effective\""
                        ).replace("\"script\"", "\"scriptTarget\""
                        ))
            }
        )
        

        for_each_end = rail.EmptyOperator(
            task_id = "for_each_end", 
        )

        gather_all_triggered_dag_runs = rail.GetVariableOperator(
            task_id = "gather_all_triggered_dag_runs",
            name=lambda : rail.result('set_variable_to_store_run_id')['name']
        )

        get_runids = rail.PythonOperator(
            task_id = "get_runids",
            python_callable=lambda : rail.result('gather_all_triggered_dag_runs') if rail.result('gather_all_triggered_dag_runs')['value'] else []
        )

        wait_for_all_triggered_dag_runs = rail.WaitForDagRunsSensor(
            task_id = "wait_for_all_triggered_dag_runs",
            dag_runs="{{ result('get_runids') }}",
            retries = 0,
            execution_timeout = timedelta(days=14)
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Add",
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


        is_rehire_true >> rail.Label("Yes") >> process_each_assigned_timeoffs
        process_each_assigned_timeoffs >> is_timeoff_not_holiday_sick_leave >> rail.Label("Yes") >> trigger_rehire_timeoff_assignment >> add_dag_run_id_to_wait1 >> end_process_each_assigned_timeoffs
        is_timeoff_not_holiday_sick_leave >> is_timeoff_name_usa_holiday >> rail.Label("Yes") >> process_rehire_usa_holiday >> add_dag_run_id_to_wait2 >> end_process_each_assigned_timeoffs
        is_timeoff_name_usa_holiday >> rail.Label("No") >> is_timeoff_name_us_sick_time >> rail.Label(
            "yes") >> trigger_sick_non_california_timeoff_dag >> add_dag_run_id_to_wait4 >> end_process_each_assigned_timeoffs
        process_each_assigned_timeoffs >> end_process_each_assigned_timeoffs
        is_timeoff_name_us_sick_time >> rail.Label("No") >> end_process_each_assigned_timeoffs

        is_rehire_true >> rail.Label("No") >> process_no_accrual
        is_schedule_changed_and_not_rehire_not_ia_updated >> rail.Label("Yes") >> process_usa_holiday_and_sick_timeoffs >> end_process_each_assigned_timeoffs2


        is_schedule_changed_and_not_rehire_not_ia_updated >> rail.Label("No") >> has_any_timeoff_to_be_assign 
        process_no_accrual >> process_no_accrual_sick_timeoff >> wait_for_no_accrual >> is_schedule_changed_and_not_rehire_not_ia_updated

        process_usa_holiday_and_sick_timeoffs >> can_process_timeoff >> rail.Label("Yes") >> is_timeoff_name_usa_holiday2_dummy >> is_timeoff_name_usa_holiday2 >> rail.Label("Yes") >> process_usa_holiday_timeoff >> add_dag_run_id_to_wait5 >> end_process_each_assigned_timeoffs2
        can_process_timeoff >> rail.Label("Yes") >> end_process_each_assigned_timeoffs2

        is_timeoff_name_usa_holiday2 >> rail.Label("No") >> is_timeoff_name_us_sick_time2 >> rail.Label("No") >> end_process_each_assigned_timeoffs2
        is_timeoff_name_us_sick_time2 >> rail.Label("Yes") >> trigger_sick_non_california_timeoff_dag2 >> add_dag_run_id_to_wait7 >> end_process_each_assigned_timeoffs2

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> date_to_considered
        set_variable_to_store_run_id >> date_to_considered >> get_all_timeoffs >> query_timeoff_data >> has_any_data_found >> rail.Label("No") >> dummy_no_task_has_any_data_found >> empty_gather_all_triggered_dag_runs
        has_any_data_found >> rail.Label("Yes") >> get_required_details >> has_any_timeoff_to_assign >> rail.Label("No") >> dummy_no_task_has_any_timeoff_to_assign >> empty_gather_all_triggered_dag_runs
        has_any_timeoff_to_assign >> rail.Label("Yes") >> get_user_timeoff_policy_summary >> get_required_details_2 >> date_to_use >> is_rehire_true
        end_process_each_assigned_timeoffs >> process_no_accrual
        has_any_timeoff_to_be_assign >> rail.Label("No") >> dummy_no_task_has_any_timeoff_to_be_assign >> empty_gather_all_triggered_dag_runs
        end_process_each_assigned_timeoffs2 >> has_any_timeoff_to_be_assign
        has_any_timeoff_to_be_assign >> rail.Label("Yes") >> assign_timeoff_to_user >> for_each_timeoff

        for_each_timeoff >> for_each_end

        for_each_timeoff >> is_timeoff_name_not_us_holiday_us_sick_leave >> rail.Label("Yes") >> is_ia_updated_1 >> rail.Label("no") >> get_default_timeoff_policy \
            >> has_any_policies >> rail.Label("Yes") >> update_timeoff_policies >> for_each_end
        has_any_policies >> rail.Label("No") >> for_each_end

        is_ia_updated_1 >> rail.Label("Yes") >> is_ia_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment >> add_dag_run_id_to_wait8 >> for_each_end
        is_ia_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> add_dag_run_id_to_wait9 >> for_each_end

        is_timeoff_name_not_us_holiday_us_sick_leave >> rail.Label("No") >> is_timeoff_usa_sick_time >> trigger_sick_non_california_timeoff >> add_dag_run_id_to_wait11 >> for_each_end >> empty_gather_all_triggered_dag_runs
        is_timeoff_usa_sick_time >> rail.Label("No") >> empty_is_timeoff_usa_holiday >> is_timeoff_usa_holiday >> rail.Label("No") >> for_each_end
        is_timeoff_usa_holiday >> rail.Label("Yes") >> trigger_usa_holiday_timeoff_child >> add_dag_run_id_to_wait12 >> for_each_end


        empty_gather_all_triggered_dag_runs >> gather_all_triggered_dag_runs >> get_runids >> wait_for_all_triggered_dag_runs >> catch_and_log_error

    return dag

rail.for_each_instance(create_update_user_timeoff_assignment_dag)
