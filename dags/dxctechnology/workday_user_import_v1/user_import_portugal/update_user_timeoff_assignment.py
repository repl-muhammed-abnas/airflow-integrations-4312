from datetime import timedelta
from json import dumps, loads
import rail
from dxctechnology.workday_user_import_v1.user_import_global.utils import custom_methods as gbl_custom_methods  
from dxctechnology.workday_user_import_v1.user_import_global.utils import request_payload
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import convert_json_date_to_date, get_json_date_from_date
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_json_date_from_date_str
from airflow.exceptions import AirflowException
from airflow.models import Variable


def create_update_user_timeoff_assignment_dag(config):
    
    with rail.create_airflow_dag(
        dag_id = config.portugal_update_user_timeoff_assignment_dag_id,
        description = "add user",
        max_active_runs = 10,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_portugal, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_timeoff_data_from_mapper"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_timeoff_data_from_mapper",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        def get_timeoff_data_from_mapper_callable(dag_run):
            country = dag_run.conf['country']
            source = dag_run.conf['parent_company_code']
            work_shift = dag_run.conf['workshift']
            gender = dag_run.conf['gender']
            time_off_data = list(filter(lambda row:  row['Type'] == "Timeoff" and
                                row['Function'] == "Workday User Sync" and
                                row['Country'] == country  and
                                row['Source'] == source and
                                row['personnelsubarea'] == work_shift and
                                row['employeegroup'] == gender,config.MAPPER))
            rail.set_result(key="has_data", val=bool(time_off_data))
            return time_off_data


        get_timeoff_data_from_mapper = rail.PythonOperator(
            task_id = "get_timeoff_data_from_mapper",
            python_callable=get_timeoff_data_from_mapper_callable
        )

        has_any_data = rail.IfOperator(
            task_id = "has_any_data",
            test=lambda : len(rail.result("get_timeoff_data_from_mapper")) > 0,
            yes_task="get_all_timeoffs",
            no_task="catch_and_log_error"
        )

        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id = "get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        def get_mapper_timeoff_types_uri_callable(dag_run):
            replicon_timeoff_types = rail.result("get_all_timeoffs")
            mapper_timeoff_types = rail.result("get_timeoff_data_from_mapper")

            return_data = list(map( lambda row:
                {
                    **row,
                    **{
                        "timeoff_type_name": row['Value'],
                        "timeoff_type_uri": rail.find_first_by_attr_and_get_attr(
                            replicon_timeoff_types,
                            "name",
                            row['Value'],
                            'uri')
                    }
                }
            , mapper_timeoff_types))
            rail.set_result(key="unique_to_data_uri", val=list(set([i['timeoff_type_uri'] for i in return_data if bool(i['timeoff_type_uri'])])))
            rail.set_result(
                key="is_any_timeoff_present_in_replicon",
                val=bool(list(filter(lambda timeoff: bool(timeoff['timeoff_type_uri']), return_data))))
            return return_data

        get_mapper_timeoff_types_uri = rail.PythonOperator(
            task_id = "get_mapper_timeoff_types_uri",
            python_callable=get_mapper_timeoff_types_uri_callable
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda: (rail.result("get_mapper_timeoff_types_uri",'is_any_timeoff_present_in_replicon')),
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

        def get_required_details_callable():
            _get_mapper_timeoff_types_uri = rail.result("get_mapper_timeoff_types_uri")
            _timeoffs_available_in_replicon = list(filter(lambda item: bool(item['timeoff_type_uri']), _get_mapper_timeoff_types_uri))
            current_assigned_timeoffs = rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType']
            timeoffs_to_assign = list(filter(lambda _timeoff: _timeoff['status'] == 'No' ,map(
                            lambda timeoff: {
                                "name": timeoff['timeoff_type_name'],
                                "enabled":rail.find_first_by_attr_and_get_attr(
                                    current_assigned_timeoffs,
                                    'timeOffType.uri',
                                    timeoff['timeoff_type_uri'],
                                    'isTimeOffAllowedAgainstThisTimeOffType'
                                ),
                                "uri": timeoff['timeoff_type_uri'],
                                "status":"Yes" if (rail.find_first_by_attr_and_get_attr(
                                    current_assigned_timeoffs,
                                    'timeOffType.uri',
                                    timeoff['timeoff_type_uri'],
                                    'timeOffType.name',
                                    None
                                )) else "No"
                            }
                        ,_timeoffs_available_in_replicon)))
            
            timeoffs_to_disable = list(filter(lambda _timeoff: _timeoff['status'] == 'No',map(lambda timeoff: {
                "name": timeoff['timeOffType']['name'],
                "uri": timeoff['timeOffType']['uri'],
                "enabled": timeoff['isTimeOffAllowedAgainstThisTimeOffType'],
                "policy": timeoff['policySetSchedule'],
                "status": ("Yes" if rail.find_first_by_attr_and_get_attr(
                    _get_mapper_timeoff_types_uri,
                    "timeoff_type_uri",
                    timeoff['timeOffType']['uri'],
                    'timeoff_type_name',
                    None
                ) else "No")

            }, current_assigned_timeoffs)))
            
            return {
                "timeoffs_to_assign": timeoffs_to_assign,
                "timeoffs_to_disable": timeoffs_to_disable
            }

        get_required_details = rail.PythonOperator(
            task_id = "get_required_details",
            python_callable = get_required_details_callable 
        )

        is_user_rehire = rail.IfOperator(
            task_id = "is_user_rehire",
            test=lambda dag_run : dag_run.conf['rehire'].lower() == "yes",
            yes_task="trigger_rehire_timeoff_assignment",
            no_task="process_timeoff_no_accrual"
        )

        def get_rehire_timeoff_types():
            return [row for row in rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'] if row['policySetSchedule']]

        def get_json_conf():
            dag_run_conf = rail.get_dag_run_conf()
            return rail.write_json_artifact(dag_run_conf)

        trigger_rehire_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_rehire_timeoff_assignment",
            trigger_dag_id = config.workday_user_import_portugal_users_update_user_rehire_timeoff_process_child_dag,
            items=get_rehire_timeoff_types,
            conf= lambda dag_run, item : {
                    "timeoff_type_uri": item['timeOffType']['uri'],
                    "current_timeoff_policies": item['policySetSchedule'],
                    "timeoff_type_name": item['timeOffType']['name'],
                    "json_formatted_dates": {
                        "start_date": gbl_custom_methods.get_todays_date_in_json(),
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
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        def get_user_end_date_to_use_for_no_accrual(dag_run):
            if dag_run.conf['ia_updated'] in [True, 'true', 'True']:
                if dag_run.conf['file_data']['is_ia'] in [1,'1']:
                    return dag_run.conf['ia_start_date']
                if dag_run.conf['file_data']['is_ia'] in [0,'0']:
                    return get_json_date_from_date(convert_json_date_to_date(dag_run.conf['ia_end_date']) + timedelta(days=1))
            return request_payload.get_todays_date_in_json()

        process_timeoff_no_accrual = rail.TriggerDagRunForEachItemOperator(
                task_id="process_timeoff_no_accrual",
                items=lambda:[i for i in rail.result(
                    "get_required_details")['timeoffs_to_disable'] if i['policy']],
                trigger_dag_id=config.process_time_off_accrual,
                conf=lambda dag_run, item: {
                    "end_date": convert_json_date_to_date(get_user_end_date_to_use_for_no_accrual(dag_run)).strftime("%Y-%d-%m"),
                    "timeoff_type_uri": item['uri'],
                    "timeoff_type_name": item['name'],
                    "policy_set": dumps(item['policy']).replace("[[{", "[{").replace("}]]", "}]"),
                    "user_end_date_json": get_user_end_date_to_use_for_no_accrual(dag_run),
                    "user_uri": dag_run.conf['user_uri'],
                    "prevent_balance_overdraw_uri":dag_run.conf['prevent_balance_overdraw_uri'],
                    "starting_balance_set_to_uri": dag_run.conf['starting_balance_set_to_uri']
                },
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )
        
        wait_for_process_timeoff_no_accrual = rail.WaitForDagRunsSensor(
                task_id="wait_for_process_timeoff_no_accrual",
                dag_runs="{{result('process_timeoff_no_accrual')}}",
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )
        
        get_no_aural_errors = rail.GatherResultsFromDagRunsOperator(
            task_id = "get_no_aural_errors",
            dag_runs="{{result('process_timeoff_no_accrual')}}",
            dagrun_task_id="catch_errors",
            flatten=True
        )

        has_any_no_aural_failure = rail.IfOperator(
            task_id = "has_any_no_aural_failure",
            test = "{{result('get_no_aural_errors') | is_truthy}}",
            yes_task = "fail_dag_run",
            no_task="has_timeoff_types_to_assign_to_user"
        )

        fail_dag_run = rail.FailOperator(
            task_id = "fail_dag_run",
            message="{{result('get_no_aural_errors')}}"
        )

        has_timeoff_types_to_assign_to_user = rail.IfOperator(
            task_id="has_timeoff_types_to_assign_to_user",
            test=lambda: len([i['uri'] for i in rail.result("get_required_details")['timeoffs_to_assign'] if i['uri']]) > 0,
            yes_task="assign_timeoff_to_user",
            no_task="for_each_timeoff"
        )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "timeOffTypeUris":[i['uri']for i in rail.result("get_required_details")['timeoffs_to_assign'] if i['uri']]
            }
        )

        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items=lambda dag_run: [timeoff for timeoff in rail.result("get_required_details")['timeoffs_to_assign'] if timeoff['uri']],
            start_task="timeoff_type_uri_to_use",
            end_task="empty_process_special_timeoff"
        )

        def is_timeoff_type_prt_vacation_current_year_and_work_week_bps_bpsot_test(dag_run):
            if rail.result("for_each_timeoff")['name'] == "[PRT] Vacation Current Year":
                if dag_run.conf['workshift'] in ['BPS', 'BPSOT']:
                    return True
            return False
            
        def timeoff_type_uri_to_use_callable(dag_run):
            timeoff_type_uri = rail.result("for_each_timeoff")['uri']
            # in workato the 2nd condition is not present it's reversed here
            # is_timeoff_type_prt_vacation_current_year and else of workshift not BPS OR BPSOT
            if is_timeoff_type_prt_vacation_current_year_and_work_week_bps_bpsot_test(dag_run):
                _timeoff_type_uri = rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), "name", "[PRT] Vacation BPS BPSOT", 'uri')
                if not _timeoff_type_uri:
                    raise AirflowException("""Placeholder timeoff type "[PRT] Vacation BPS BPSOT" is not present in Replicon""")
                return _timeoff_type_uri
            return timeoff_type_uri

        timeoff_type_uri_to_use = rail.PythonOperator(
            task_id = "timeoff_type_uri_to_use",
            python_callable=timeoff_type_uri_to_use_callable
        )

        is_ia_updated = rail.IfOperator(
            task_id = "is_ia_updated",
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
                "contineous_service_date": dag_run.conf['hire_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "secondary_timeoff_uri": rail.result("timeoff_type_uri_to_use"),
                "policy": [],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['ia_start_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        add_dag_run_id_for_wait = rail.PythonOperator(
            task_id = "add_dag_run_id_for_wait",
            python_callable=lambda: (rail.result("add_dag_run_id_for_wait") + [rail.result("trigger_ia_one_timeoff_assignment")]
                                     )if rail.result("add_dag_run_id_for_wait") else [rail.result("trigger_ia_one_timeoff_assignment")]
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
                    "start_date": dag_run.conf['hire_date'],
                    "ia_end_date": dag_run.conf['ia_end_date']
                }
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            retries=0
        )

        add_dag_run_id_for_wait2 = rail.PythonOperator(
            task_id = "add_dag_run_id_for_wait2",
            python_callable=lambda: (rail.result("add_dag_run_id_for_wait2") + [rail.result("trigger_ia_zero_timeoff_assignment")]
                                     ) if rail.result("add_dag_run_id_for_wait2") else [rail.result("trigger_ia_zero_timeoff_assignment")]
        )

        def get_default_timeoff_policy_payload(dag_run):
            timeoff_type_uri = rail.result("timeoff_type_uri_to_use")
            return {
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": timeoff_type_uri
                }
            }

        get_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=get_default_timeoff_policy_payload
        )

        def get_update_timeoff_policies_payload(dag_run):
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                },
                "policySetScheduleEntries": loads(dumps(rail.result("get_default_timeoff_policy")
                            ).replace("null", "\"effective\""
                        ).replace("\"script\"", "\"scriptTarget\""
                        )) if rail.result("get_default_timeoff_policy") else []
            }

        update_timeoff_policies = rail.RepliconServiceOperator(
            task_id = "update_timeoff_policies",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_update_timeoff_policies_payload
        )

        empty_end_for_each= rail.EmptyOperator(
            task_id = "empty_process_special_timeoff"
        )

        gather_runids_for_wait = rail.PythonOperator(
            task_id = "gather_runids_for_wait",
            python_callable = lambda: (rail.result("add_dag_run_id_for_wait") or []) +  (rail.result("add_dag_run_id_for_wait2") or [])
        )

        wait_for_user_timeoff_update_completion = rail.WaitForDagRunsSensor(
            task_id = "wait_for_user_timeoff_update_completion",
            dag_runs="{{result('gather_runids_for_wait')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
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
        can_run_batch_task >> rail.Label("No") >> get_timeoff_data_from_mapper
        get_timeoff_data_from_mapper >> has_any_data >> rail.Label("No") >> catch_and_log_error
        has_any_data >> rail.Label("Yes") >> get_all_timeoffs >> get_mapper_timeoff_types_uri >> has_any_timeoff_to_assign
        has_any_timeoff_to_assign >> rail.Label("No") >> catch_and_log_error
        has_any_timeoff_to_assign >> rail.Label("Yes") >> get_user_timeoff_policy_summary >> get_required_details >> is_user_rehire >> rail.Label("Yes") >> trigger_rehire_timeoff_assignment
        trigger_rehire_timeoff_assignment >> wait_for_trigger_rehire_timeoff_assignment >> process_timeoff_no_accrual
        is_user_rehire >> rail.Label("No") >> process_timeoff_no_accrual >> wait_for_process_timeoff_no_accrual >> get_no_aural_errors >> has_any_no_aural_failure >> rail.Label("Yes") >> fail_dag_run >> catch_and_log_error
        has_any_no_aural_failure >> rail.Label("No") >> has_timeoff_types_to_assign_to_user >> rail.Label("Yes") >> assign_timeoff_to_user >> for_each_timeoff
        has_timeoff_types_to_assign_to_user >> rail.Label("No") >> for_each_timeoff
        for_each_timeoff >> timeoff_type_uri_to_use >> is_ia_updated >> rail.Label("No") >> get_default_timeoff_policy >> update_timeoff_policies >> empty_end_for_each
        is_ia_updated >> rail.Label("Yes") >> is_ia_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment >> add_dag_run_id_for_wait >> empty_end_for_each
        is_ia_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> add_dag_run_id_for_wait2 >> empty_end_for_each
        for_each_timeoff >> empty_end_for_each >> gather_runids_for_wait >> wait_for_user_timeoff_update_completion >> catch_and_log_error
        return dag
    
rail.for_each_instance(create_update_user_timeoff_assignment_dag)
