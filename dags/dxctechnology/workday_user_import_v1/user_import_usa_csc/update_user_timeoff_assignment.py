from datetime import timedelta
from json import dumps, loads
import pendulum
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import get_date_to_use_for_no_accrual
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_date_in_json, get_json_date_from_date_str

def create_update_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.usa_csc_update_user_timeoff_assignment_dag_id,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_run_update_to_assignment
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_us_csc, default_var='true').lower() == 'true',
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
            ia_date_to_use = get_date_to_use_for_no_accrual(dag_run, default_return="NA")
            rail.set_result(key = "ia_date_to_use", val = (ia_date_to_use if ia_date_to_use != 'NA' else ''))
            return pendulum.now("America/Los_Angeles").format('Y-DD-MM')


        date_to_considered = rail.PythonOperator(
            task_id = "date_to_considered",
            python_callable = date_to_considered_callable
        )

        def is_old_and_new_ee_grp_3_test(dag_run):
            if dag_run.conf['old_ee_group'] == '3' and dag_run.conf['employee_group'] == '3':
                return True
            return False

        is_old_and_new_ee_grp_3 = rail.IfOperator(
            task_id = "is_old_and_new_ee_grp_3",
            test=is_old_and_new_ee_grp_3_test,
            yes_task="catch_and_log_error",
            no_task="is_old_ee_group_not_3_and_new_ee_grp_3"
        )

        def is_old_ee_group_not_3_and_new_ee_grp_3_test(dag_run):
            if dag_run.conf['old_ee_group'] != '3' and dag_run.conf['employee_group'] == '3':
                return True
            return False

        is_old_ee_group_not_3_and_new_ee_grp_3 = rail.IfOperator(
            task_id = "is_old_ee_group_not_3_and_new_ee_grp_3",
            test=is_old_ee_group_not_3_and_new_ee_grp_3_test,
            yes_task="disable_timeoffs",
            no_task="get_all_timeoffs"
        )

        disable_timeoffs = rail.RepliconServiceOperator(
            task_id = "disable_timeoffs",
            endpoint = "/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUris": []
                }
        )

        def _get_user_timeoff_policy_summary_for_no_accrual_data_handler(response):
            if not response:
                return []
            rail.set_result(key="response", val=response)
            return list(filter(lambda x:x['enabled']in [True, 'true', 'True'] and bool(x['policy']), map(lambda item: {
                                            "name": item['timeOffType']['displayText'],
                                            "enabled":item['isTimeOffAllowedAgainstThisTimeOffType'],
                                            "uri": item['timeOffType']['uri'],
                                            "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
                                        } ,response['policiesByTimeOffType'])))

        get_user_timeoff_policy_summary_for_no_accrual = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_policy_summary_for_no_accrual",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri" : "{{dag_run.conf.user_uri}}"
            },
            data_handler=_get_user_timeoff_policy_summary_for_no_accrual_data_handler
        )

        process_no_accrual = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_no_accrual",
            items=lambda: [timeoff for timeoff in rail.result(
                    "get_user_timeoff_policy_summary_for_no_accrual") if timeoff['policy']],
                trigger_dag_id=config.process_time_off_accrual,
                conf=lambda dag_run, item: {
                    **dag_run.conf,
                    **{
                        "timeoff_type_uri": item['uri'],
                        "policy_set": dumps(item['policy']).replace("[[{", "[{").replace("}]]", "}]"),
                        "user_end_date_json": get_todays_date_in_json()
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

        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id = "get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        def get_timeoff_list_from_mapper(dag_run):
            list1 =  list(filter(lambda row: row['Type']=='Timeoff' and\
                row['Country']==dag_run.conf['country'] and\
                row['Function']=='Workday User Sync' and\
                row['Source'] == dag_run.conf['parent_company_code'] and\
                row['URI'] == (dag_run.conf['personnal_sub_area'] if dag_run.conf['personnal_sub_area'] in ["U04A","U02A","U05A","U06A"] else "All - Others") and\
                row['personnelsubarea'] == dag_run.conf['psg'] and\
                row['status'] == ((dag_run.conf['state'] if dag_run.conf['state'] in ["U04A","U02A","U05A","U06A"] else "All - Others")
                     if dag_run.conf['country'] == "United States of America" else dag_run.conf['country']) and\
                row['employeegroup'] == dag_run.conf['employee_group'] and\
                row['employeesubgroup'] == dag_run.conf['employee_sub_group'], config.MAPPER))

            list2=[]
            if dag_run.conf['state'] in ["California","Colorado","Nevada","Puerto Rico"] or dag_run.conf['country'] == "Puerto Rico":
                list2 = list(filter(lambda row: row['Type']=='Timeoff' and\
                    row['Country']==dag_run.conf['country'] and\
                    row['Function']=='Workday User Sync' and\
                    row['Source'] == dag_run.conf['parent_company_code'] and\
                    row['URI'] == "" and\
                    row['personnelsubarea'] == "" and\
                    row['status'] == (dag_run.conf['state'] if dag_run.conf['state'] in ["U04A","U02A","U05A","U06A"] else "All - Others")
                        if dag_run.conf['country'] == "United States of America" else dag_run.conf['country'] and\
                    row['employeegroup'] == "" and\
                    row['employeesubgroup'] == dag_run.conf['employee_sub_group'], config.MAPPER))
        
            return list1+list2

        query_timeoff_data = rail.PythonOperator(
            task_id = "query_timeoff_data",
            python_callable=lambda dag_run: get_timeoff_list_from_mapper(dag_run)
        )

        has_any_data_found = rail.IfOperator(
            task_id = "has_any_data_found",
            test=lambda: bool(rail.result("query_timeoff_data")),
            yes_task="get_required_details",
            no_task="catch_and_log_error"
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
                "unique_uri_data" : list(set(i['uri'] for i in return_data)),
                "unique_uri_data_len" : len(set(i['uri'] for i in return_data))
            }

        get_required_details = rail.PythonOperator(
            task_id = "get_required_details",
            python_callable=get_required_details_callable
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda : rail.result("get_required_details")['unique_uri_data_len'] > 0,
            yes_task="get_user_timeoff_policy_summary",
            no_task="catch_and_log_error"
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

            assignedtimeofftypes = list(filter(lambda x:x['enabled']==True, map(lambda item: {
                                            "name": item['timeOffType']['displayText'],
                                            "enabled":item['isTimeOffAllowedAgainstThisTimeOffType'],
                                            "uri": item['timeOffType']['uri'],
                                            "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
                                        },user_timeoff_policy_summary['policiesByTimeOffType'])))

            raw_finaltimeofflist = rail.result("get_required_details")['return_data']

            finaltimeofflist = [i["uri"] for i in raw_finaltimeofflist]

            timeofftypestobeassigned = list(filter(lambda item2: item2['status'] == "No",map(lambda item: {
                                            "name": item['name'],
                                            "enabled": rail.find_first_by_attr_and_get_attr(assignedtimeofftypes, 'uri', item['uri'], 'enabled'),
                                            "uri": item['uri'],
                                            "status": "Yes" if rail.find_first_by_attr_and_get_attr(assignedtimeofftypes, 'uri', item['uri'], 'name') else "No" 
                                        }, raw_finaltimeofflist)))

            timeofftypestobedisabled = list(filter(lambda item: item['status']== "No", map(lambda item2: {
                    "name":item2['name'],
                    "enabled":item2['enabled'],
                    "uri":item2['uri'],
                    "policy":item2['policy'],
                    "status": "Yes" if item2['uri'] in finaltimeofflist else "No"
            }, assignedtimeofftypes)))
 
            return {
                "assignedtimeofftypes": assignedtimeofftypes,
                "finaltimeofflist": finaltimeofflist,
                "timeofftypestobeassigned": timeofftypestobeassigned,
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
            return rail.result(task_id)['name'] not in ['[USA] 02-CSC Holiday', '[USA] 03-CSC Sick Time', '[PR] 04-Vacation Accrued']

        is_rehire_true = rail.IfOperator(
            task_id = "is_rehire_true",
            test="{{dag_run.conf.rehire == 'Yes'}}",
            yes_task="process_each_assigned_timeoffs",
            no_task="is_schedule_changed_and_not_rehire_not_ia_updated"
        )

        process_each_assigned_timeoffs = rail.ForEachOperator(
            task_id = "process_each_assigned_timeoffs",
            items=lambda : [timeoff for timeoff in rail.result("get_required_details_2")['assignedtimeofftypes'] if timeoff['policy']],
            start_task="is_timeoff_not_holiday_sick__vacation_accrued_leave",
            end_task="end_process_each_assigned_timeoffs"
        )

        is_timeoff_not_holiday_sick__vacation_accrued_leave = rail.IfOperator(
            task_id = "is_timeoff_not_holiday_sick__vacation_accrued_leave",
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
            trigger_dag_id = config.usa_csc_rehire_timeoff_assignment,
            conf= lambda dag_run, item : {
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri'],
                "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"],
                "timeoff_type_uri": rail.result('process_each_assigned_timeoffs')['uri'],
                "current_timeoff_policies": rail.result('process_each_assigned_timeoffs')['policy'],
                "timeoff_type_name": rail.result('process_each_assigned_timeoffs')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "continuous_service_date": dag_run.conf['json_formatted_dates']['hire_date']
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

        add_dag_run_id_to_wait1 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait1",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_rehire_timeoff_assignment"),
            append=True
        )

        is_timeoff_name_usa_holiday = rail.IfOperator(
            task_id = "is_timeoff_name_usa_holiday",
            test = lambda: rail.result("process_each_assigned_timeoffs")['name'] == "[USA] 02-CSC Holiday",
            yes_task="process_rehire_usa_holiday",
            no_task="is_timeoff_name_us_sick_time"
        )

        process_rehire_usa_holiday = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_rehire_usa_holiday",
            items = [1],
            trigger_dag_id = config.usa_csc_us_holiday_user_timeoff_assignment_dag_id,
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
                "contineous_service_date": dag_run.conf['start_date'],
                "timeoff_uri": rail.result('process_each_assigned_timeoffs')['uri'],
                "timeoff_name": rail.result('process_each_assigned_timeoffs')['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Update",
                "policy_sets":  rail.result('process_each_assigned_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['start_date'],
                "fte": dag_run.conf['fte'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],

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
            test=lambda: rail.result("process_each_assigned_timeoffs")['name'] == "[USA] 03-CSC Sick Time",
            yes_task="is_state_california",
            no_task="is_timeoff_name_vacation_accrued"
        )

        is_state_california = rail.IfOperator(
            task_id = "is_state_california",
            test="{{ dag_run.conf.state == 'California'}}",
            yes_task="trigger_sick_california_timeoff_dag",
            no_task="trigger_sick_non_california_timeoff_dag"
        )

        trigger_sick_california_timeoff_dag = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_sick_california_timeoff_dag",
            items = [1],
            trigger_dag_id = config.usa_csc_us_sick_leave_california_user_timeoff_assignment_dag_id,
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
                "contineous_service_date": dag_run.conf['start_date'],
                "timeoff_uri": rail.result('process_each_assigned_timeoffs')['uri'],
                "timeoff_name": rail.result('process_each_assigned_timeoffs')['name'],
                "secondary_timeoff_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), "name", "[USA] CSC CA Sick Time", 'uri', ''),
                "Secondarytimeoffname": "[USA] CSC CA Sick Time",
                "caller": "Update",
                "policy_sets": rail.result('process_each_assigned_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['start_date'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
            }
        )

        add_dag_run_id_to_wait3 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait3",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_sick_california_timeoff_dag"),
            append=True
        )

        trigger_sick_non_california_timeoff_dag = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_sick_non_california_timeoff_dag",
            items = [1],
            trigger_dag_id = config.usa_csc_us_sick_leave_non_california_user_timeoff_assignment_dag_id,
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
                "contineous_service_date": dag_run.conf['start_date'],
                "timeoff_uri": rail.result('process_each_assigned_timeoffs')['uri'],
                "timeoff_name": rail.result('process_each_assigned_timeoffs')['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Update",
                "policy_sets": rail.result('process_each_assigned_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['start_date'],
                "fte": dag_run.conf['fte'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
            }
        )

        add_dag_run_id_to_wait4 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait4",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_sick_non_california_timeoff_dag"),
            append=True
        )

        is_timeoff_name_vacation_accrued = rail.IfOperator(
            task_id = "is_timeoff_name_vacation_accrued",
            test = lambda: rail.result("process_each_assigned_timeoffs")['name'] == "[PR] 04-Vacation Accrued",
            yes_task="process_rehire_vacation_accrued",
            no_task="end_process_each_assigned_timeoffs"
        )

        process_rehire_vacation_accrued = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_rehire_vacation_accrued",
            items = [1],
            trigger_dag_id = config.usa_csc_us_puerto_rico_user_timeoff_assignment_dag_id,
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
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result('process_each_assigned_timeoffs')['uri'],
                "timeoff_name": rail.result('process_each_assigned_timeoffs')['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Updated",
                "policy_sets": rail.result('process_each_assigned_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['start_date'],
                "fte": dag_run.conf['fte'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
            }
        )

        add_dag_run_id_to_wait_vacation = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait_vacation",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("process_rehire_vacation_accrued"),
            append=True
        )

        end_process_each_assigned_timeoffs = rail.EmptyOperator(
            task_id = "end_process_each_assigned_timeoffs"
        )
        
        def is_schedule_changed_and_not_rehire_not_ia_updated_test(dag_run):
            if dag_run.conf['schedule_change']:
                if (dag_run.conf['schedule_change'].lower() == "yes")and dag_run.conf['rehire'] =="No":\
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
            start_task="is_timeoff_name_usa_holiday2",
            end_task="end_process_each_assigned_timeoffs2"
        )

        is_timeoff_name_usa_holiday2 = rail.IfOperator(
            task_id = "is_timeoff_name_usa_holiday2",
            test = lambda: rail.result("process_usa_holiday_and_sick_timeoffs")['name'] == "[USA] 02-CSC Holiday",
            yes_task="process_usa_holiday_timeoff",
            no_task="is_timeoff_name_us_sick_time2"
        )

        process_usa_holiday_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_usa_holiday_timeoff",
            items = [1],
            trigger_dag_id = config.usa_csc_us_holiday_user_timeoff_assignment_dag_id,
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
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("process_usa_holiday_and_sick_timeoffs")['uri'],
                "timeoff_name": rail.result("process_usa_holiday_and_sick_timeoffs")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Update",
                "policy_sets": rail.result("process_usa_holiday_and_sick_timeoffs")['policy'],
                "schedule_changed_date": dag_run.conf['schedule_changed_date'],
                "fte": dag_run.conf['fte'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
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
            test=lambda: rail.result("process_usa_holiday_and_sick_timeoffs")['name'] == "[USA] 03-CSC Sick Time",
            yes_task="is_state_california2",
            no_task="is_timeoff_name_vacation_accrued_2"
        )

        is_state_california2 = rail.IfOperator(
            task_id = "is_state_california2",
            test="{{ dag_run.conf.state == 'California'}}",
            yes_task="trigger_sick_california_timeoff_dag2",
            no_task="trigger_sick_non_california_timeoff_dag2"
        )

        trigger_sick_california_timeoff_dag2 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_sick_california_timeoff_dag2",
            items = [1],
            trigger_dag_id = config.usa_csc_us_sick_leave_california_user_timeoff_assignment_dag_id,
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
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result('process_usa_holiday_and_sick_timeoffs')['uri'],
                "timeoff_name": rail.result('process_usa_holiday_and_sick_timeoffs')['name'],
                "secondary_timeoff_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), "name", "[USA] CSC CA Sick Time", 'uri', ''),
                "Secondarytimeoffname": "[USA] CSC CA Sick Time",
                "caller": "Update",
                "policy_sets": rail.result('process_usa_holiday_and_sick_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['schedule_changed_date'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
            }
        )

        add_dag_run_id_to_wait6 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait6",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_sick_california_timeoff_dag2"),
            append=True
        )

        trigger_sick_non_california_timeoff_dag2 = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_sick_non_california_timeoff_dag2",
            items = [1],
            trigger_dag_id = config.usa_csc_us_sick_leave_non_california_user_timeoff_assignment_dag_id,
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
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("process_usa_holiday_and_sick_timeoffs")['uri'],
                "timeoff_name": rail.result("process_usa_holiday_and_sick_timeoffs")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Update",
                "policy_sets": rail.result('process_usa_holiday_and_sick_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['start_date'],
                "fte": dag_run.conf['fte'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
            }
        )

        add_dag_run_id_to_wait7 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait7",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_sick_non_california_timeoff_dag2"),
            append=True
        )

        is_timeoff_name_vacation_accrued_2 = rail.IfOperator(
            task_id = "is_timeoff_name_vacation_accrued_2",
            test = lambda: rail.result("process_usa_holiday_and_sick_timeoffs")['name'] == "[PR] 04-Vacation Accrued",
            yes_task="process_rehire_vacation_accrued2",
            no_task="end_process_each_assigned_timeoffs2"
        )

        process_rehire_vacation_accrued2 = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_rehire_vacation_accrued2",
            items = [1],
            trigger_dag_id = config.usa_csc_us_puerto_rico_user_timeoff_assignment_dag_id,
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
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("process_usa_holiday_and_sick_timeoffs")['uri'],
                "timeoff_name": rail.result("process_usa_holiday_and_sick_timeoffs")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Updated",
                "policy_sets": rail.result('process_usa_holiday_and_sick_timeoffs')['policy'],
                "schedule_changed_date": dag_run.conf['start_date'],
                "fte": dag_run.conf['fte'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
            }
        )

        add_dag_run_id_to_wait_vacation2 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait_vacation2",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("process_rehire_vacation_accrued2"),
            append=True
        )

        end_process_each_assigned_timeoffs2 = rail.EmptyOperator(
            task_id = "end_process_each_assigned_timeoffs2"
        )

        has_any_timeoff_to_be_assign  = rail.IfOperator(
            task_id = "has_any_timeoff_to_be_assign",
            test = lambda : bool(rail.result("get_required_details_2")['timeofftypestobeassigned']),
            yes_task = "assign_timeoff_to_user",
            no_task = "get_all_run_ids"
        )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run :{
                "userUri": dag_run.conf['user_uri'],
                "timeOffTypeUris": rail.result("get_required_details")['unique_uri_data']
            }
        )

        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items = lambda: rail.result("get_required_details_2")['timeofftypestobeassigned'],
            start_task = "is_timeoff_name_not_holiday_sick_vacation_accrual_leave",
            end_task = "for_each_end"
        )

        is_timeoff_name_not_holiday_sick_vacation_accrual_leave = rail.IfOperator(
            task_id = "is_timeoff_name_not_holiday_sick_vacation_accrual_leave",
            test=lambda: is_timeoff_not_holiday_sick_leave_test("for_each_timeoff"),
            yes_task = "is_ia_updated",
            no_task = "is_timeoff_vacation_accrued"
        )

        is_ia_updated = rail.IfOperator(
            task_id = "is_ia_updated",
            test = lambda dag_run: dag_run.conf['ia_updated'] in [True, 'true', 'True'],
            yes_task = "is_ia_equal_1",
            no_task = "get_default_timeoff_policy_schedule_for_user"
        )

        is_ia_equal_1 = rail.IfOperator(
            task_id = "is_ia_equal_1",
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
                "contineous_service_date": dag_run.conf['start_date'],
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

        add_dag_run_id_for_wait12 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_for_wait12",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
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
                "star_date": dag_run.conf['start_date'],
                "ia_end_date": dag_run.conf['ia_end_date'],
                "country": dag_run.conf['country'],
                "personnel_subarea": "",
                "employee_group":"",
                "employee_subgroup": "",
                "contineous_service_date": dag_run.conf['start_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "secondary_timeoff_uri": None,
                "policy": [],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['start_date'],
                    "ia_end_date": dag_run.conf['ia_end_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        add_dag_run_id_for_wait13 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_for_wait13",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_ia_zero_timeoff_assignment"),
            append=True
        )

        get_default_timeoff_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy_schedule_for_user",
            endpoint = "/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                }
            }
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test=lambda: bool(rail.result("get_default_timeoff_policy_schedule_for_user")),
            yes_task="put_default_policy_to_user",
            no_task="for_each_end"
        )

        def get_put_default_policy_to_user_payload(dag_run):
            policy = loads(dumps(rail.result("get_default_timeoff_policy_schedule_for_user")
                    ).replace("null", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                ))
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                },
                "policySetScheduleEntries": policy
            }

        put_default_policy_to_user = rail.RepliconServiceOperator(
            task_id = "put_default_policy_to_user",
            endpoint = "/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = get_put_default_policy_to_user_payload
        )

        def timeoff_name_is_es_vacation_accrued():
            return rail.result("for_each_timeoff")['name'] == '[PR] 04-Vacation Accrued'

        is_timeoff_vacation_accrued = rail.IfOperator(
            task_id = "is_timeoff_vacation_accrued",
            test=timeoff_name_is_es_vacation_accrued,
            yes_task="trigger_vacation_accrued_timeoff_assignment",
            no_task="is_timeoff_sick_time"
        )

        trigger_vacation_accrued_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_vacation_accrued_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_csc_us_puerto_rico_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": rail.result('date_to_considered', 'ia_date_to_use') if rail.result('date_to_considered', 'ia_date_to_use') else dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Add",
                "policy_sets": [],
                "schedule_changed_date": None,
                "fte": dag_run.conf['fte'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
            }
        )

        add_dag_run_id_to_wait8 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait8",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_vacation_accrued_timeoff_assignment"),
            append=True
        )

        def timeoff_name_is_sick_time_test():
            return rail.result("for_each_timeoff")['name'] == '[USA] 03-CSC Sick Time'

        is_timeoff_sick_time = rail.IfOperator(
            task_id = "is_timeoff_sick_time",
            test=timeoff_name_is_sick_time_test,
            yes_task="is_states_california",
            no_task="is_timeoff_es_holiday"
        )

        def is_state_california_test(dag_run):
            return dag_run.conf['state'] == 'California'

        is_states_california = rail.IfOperator(
            task_id = "is_states_california",
            test=is_state_california_test,
            yes_task="trigger_us_sick_california_timeoff_assignment",
            no_task="trigger_us_sick_non_california_timeoff_assignment"
        )

        trigger_us_sick_california_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_us_sick_california_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_csc_us_sick_leave_california_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": rail.result('date_to_considered', 'ia_date_to_use') if rail.result('date_to_considered', 'ia_date_to_use') else dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "secondary_timeoff_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), "name", "[USA] CSC CA Sick Time", 'uri', ''),
                "Secondarytimeoffname": "[USA] CSC CA Sick Time",
                "caller": "Add",
                "policy_sets": [],
                "schedule_changed_date": None,
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
                
            }
        )

        add_dag_run_id_to_wait9 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait9",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_us_sick_california_timeoff_assignment"),
            append=True
        )

        trigger_us_sick_non_california_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_us_sick_non_california_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_csc_us_sick_leave_non_california_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": rail.result('date_to_considered', 'ia_date_to_use') if rail.result('date_to_considered', 'ia_date_to_use') else dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Add",
                "policy_sets": [],
                "schedule_changed_date": None,
                "fte": dag_run.conf['fte'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
            }
        )

        add_dag_run_id_to_wait10 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait10",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_us_sick_non_california_timeoff_assignment"),
            append=True
        )

        def timeoff_name_is_es_holiday_test():
            return rail.result("for_each_timeoff")['name'] == '[USA] 02-CSC Holiday'

        is_timeoff_es_holiday = rail.IfOperator(
            task_id = "is_timeoff_es_holiday",
            test=timeoff_name_is_es_holiday_test,
            yes_task="trigger_us_holiday_timeoff_assignment",
            no_task="for_each_end"
        )

        trigger_us_holiday_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_us_holiday_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_csc_us_holiday_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": rail.result('date_to_considered', 'ia_date_to_use')if rail.result('date_to_considered', 'ia_date_to_use') else dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Add",
                "policy_sets": [],
                "schedule_changed_date": None,
                "fte": dag_run.conf['fte'],
                "personnal_sub_area":dag_run.conf['personnal_sub_area'],
                "employee_group": dag_run.conf['employee_group'],
                "employee_sub_group":dag_run.conf['employee_sub_group'],
            }
        )

        add_dag_run_id_to_wait11 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait11",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_us_holiday_timeoff_assignment"),
            append=True
        )

        for_each_end = rail.EmptyOperator(
            task_id = "for_each_end", 
        )

        get_all_run_ids = rail.PythonOperator(
            task_id = "get_all_run_ids",
            python_callable=lambda :rail.get_dag_run_var(rail.result('set_variable_to_store_run_id')['name']) or []
        )

        get_run_ids = rail.PythonOperator(
            task_id = "get_run_ids",
            python_callable= lambda: rail.result('get_all_run_ids') if rail.result('get_all_run_ids') else []
        )

        wait_for_triggered_runs = rail.WaitForDagRunsSensor(
            task_id = "wait_for_triggered_runs",
            dag_runs="{{ result('get_run_ids') }}",
            retries = 0,
            execution_timeout = timedelta(days=14)
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
        can_run_batch_task >> rail.Label("No") >> date_to_considered >> is_old_and_new_ee_grp_3

        is_old_and_new_ee_grp_3 >> rail.Label("Yes") >> catch_and_log_error
        is_old_and_new_ee_grp_3 >> rail.Label("No") >> is_old_ee_group_not_3_and_new_ee_grp_3
        
        is_old_ee_group_not_3_and_new_ee_grp_3 >> rail.Label("Yes"
                ) >> disable_timeoffs >> get_user_timeoff_policy_summary_for_no_accrual >> process_no_accrual >> wait_for_no_accrual >> catch_and_log_error

        is_old_ee_group_not_3_and_new_ee_grp_3 >> rail.Label("No") >> get_all_timeoffs >> query_timeoff_data >> has_any_data_found
        has_any_data_found >> rail.Label("Yes") >> get_required_details
        has_any_data_found >> rail.Label('No') >> catch_and_log_error

        get_required_details >> has_any_timeoff_to_assign >> rail.Label('No') >> catch_and_log_error
        has_any_timeoff_to_assign >> rail.Label('Yes') >> get_user_timeoff_policy_summary >> get_required_details_2 >> set_variable_to_store_run_id
        set_variable_to_store_run_id >> is_rehire_true >> rail.Label("Yes") >> process_each_assigned_timeoffs
        is_rehire_true >> rail.Label("No") >> is_schedule_changed_and_not_rehire_not_ia_updated

        process_each_assigned_timeoffs >> is_timeoff_not_holiday_sick__vacation_accrued_leave >> rail.Label('No') >> is_timeoff_name_usa_holiday
        is_timeoff_not_holiday_sick__vacation_accrued_leave >> rail.Label('Yes') >>  trigger_rehire_timeoff_assignment >> add_dag_run_id_to_wait1
        add_dag_run_id_to_wait1 >> is_timeoff_name_usa_holiday >> rail.Label('No') >> is_timeoff_name_us_sick_time
        is_timeoff_name_usa_holiday >> rail.Label('Yes') >> process_rehire_usa_holiday >> add_dag_run_id_to_wait2 >> is_timeoff_name_us_sick_time
        is_timeoff_name_us_sick_time >> rail.Label('No') >> is_timeoff_name_vacation_accrued
        is_timeoff_name_us_sick_time >> rail.Label('Yes') >> is_state_california >> rail.Label('No') >> trigger_sick_non_california_timeoff_dag
        trigger_sick_non_california_timeoff_dag >> add_dag_run_id_to_wait4 >> is_timeoff_name_vacation_accrued
        is_state_california >> rail.Label('Yes') >> trigger_sick_california_timeoff_dag >> add_dag_run_id_to_wait3 >> is_timeoff_name_vacation_accrued
        is_timeoff_name_vacation_accrued >> rail.Label("Yes") >> process_rehire_vacation_accrued >> add_dag_run_id_to_wait_vacation >> end_process_each_assigned_timeoffs
        is_timeoff_name_vacation_accrued >> rail.Label("No") >> end_process_each_assigned_timeoffs

        process_each_assigned_timeoffs >> end_process_each_assigned_timeoffs >> get_all_run_ids

        is_schedule_changed_and_not_rehire_not_ia_updated >> rail.Label("Yes") >> process_usa_holiday_and_sick_timeoffs
        is_schedule_changed_and_not_rehire_not_ia_updated >> rail.Label('No') >> has_any_timeoff_to_be_assign

        process_usa_holiday_and_sick_timeoffs >> is_timeoff_name_usa_holiday2 >> rail.Label('Yes') >> process_usa_holiday_timeoff >> add_dag_run_id_to_wait5 >> is_timeoff_name_us_sick_time2
        is_timeoff_name_usa_holiday2 >> rail.Label('No') >> is_timeoff_name_us_sick_time2 >> rail.Label("Yes") >> is_state_california2
        is_timeoff_name_us_sick_time2 >> rail.Label("No") >> is_timeoff_name_vacation_accrued_2
        is_state_california2 >> rail.Label("Yes") >> trigger_sick_california_timeoff_dag2 >> add_dag_run_id_to_wait6 >> is_timeoff_name_vacation_accrued_2
        is_state_california2 >> rail.Label("No") >> trigger_sick_non_california_timeoff_dag2 >> add_dag_run_id_to_wait7 >> is_timeoff_name_vacation_accrued_2

        is_timeoff_name_vacation_accrued_2 >> rail.Label('No') >> end_process_each_assigned_timeoffs2
        is_timeoff_name_vacation_accrued_2 >> rail.Label('Yes') >> process_rehire_vacation_accrued2 >> add_dag_run_id_to_wait_vacation2 >> end_process_each_assigned_timeoffs2

        process_usa_holiday_and_sick_timeoffs >> end_process_each_assigned_timeoffs2 >> get_all_run_ids

        has_any_timeoff_to_be_assign >> rail.Label('Yes') >> assign_timeoff_to_user >> for_each_timeoff >> is_timeoff_name_not_holiday_sick_vacation_accrual_leave
        has_any_timeoff_to_be_assign >> rail.Label('No') >> get_all_run_ids

        is_timeoff_name_not_holiday_sick_vacation_accrual_leave >> rail.Label('No') >> is_timeoff_vacation_accrued
        is_timeoff_name_not_holiday_sick_vacation_accrual_leave >> rail.Label('Yes') >> is_ia_updated
        is_ia_updated >> rail.Label("Yes") >> is_ia_equal_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment >> add_dag_run_id_for_wait12 >> for_each_end
        is_ia_equal_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> add_dag_run_id_for_wait13 >> for_each_end
        is_ia_updated >> rail.Label("No") >> get_default_timeoff_policy_schedule_for_user >> has_any_policy_to_assign

        has_any_policy_to_assign >> rail.Label('Yes') >> put_default_policy_to_user >> for_each_end
        has_any_policy_to_assign >> rail.Label('No') >> for_each_end

        is_timeoff_vacation_accrued >> rail.Label('Yes') >> trigger_vacation_accrued_timeoff_assignment >> add_dag_run_id_to_wait8 >> is_timeoff_sick_time
        is_timeoff_vacation_accrued >> rail.Label('No') >> is_timeoff_sick_time >> rail.Label('No') >> is_timeoff_es_holiday

        is_timeoff_sick_time >> rail.Label('Yes') >> is_states_california >> rail.Label('No') >> trigger_us_sick_non_california_timeoff_assignment >> add_dag_run_id_to_wait10 >> is_timeoff_es_holiday
        is_states_california >> rail.Label('Yes') >> trigger_us_sick_california_timeoff_assignment >> add_dag_run_id_to_wait9 >> is_timeoff_es_holiday

        is_timeoff_es_holiday >> rail.Label('Yes') >> trigger_us_holiday_timeoff_assignment >> add_dag_run_id_to_wait11 >> for_each_end
        is_timeoff_es_holiday >> rail.Label('No') >> for_each_end
 
        for_each_timeoff >> for_each_end >> get_all_run_ids >> get_run_ids >> wait_for_triggered_runs >> catch_and_log_error

        return dag

rail.for_each_instance(create_update_user_timeoff_assignment_dag)
