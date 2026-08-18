from datetime import datetime, timedelta
from pendulum import datetime as dt, now as pendulum_now
from dateutil.relativedelta import relativedelta
from json import dumps, loads
import rail
from airflow.models import Variable

from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import get_tenure_value
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_todays_date_for_timezone_in_json
from dxctechnology.workday_user_import.user_import_global.utils import custom_methods as gbl_custom_methods
from dxctechnology.workday_user_import.user_import_india.utils import custom_methods

DATE_FORMAT = "%Y-%d-%m"
null = None

# pylint: disable=too-many-statements
def create_update_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id = config.india_update_user_timeoff_assignment_dag_id,
        description = "DXC Workday User Import INDIA - Process Update User TimeOff Assignment",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.max_active_run_update_user_timeoff_assignment_india
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_india, default_var='true').lower() == 'true',
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
            source = dag_run.conf['source']
            job_level = dag_run.conf['job_level']
            time_off_data = list(filter(lambda row:  row['Type'] == "Timeoff" and
                                row['Function'] == "Workday User Sync" and
                                row['Country'] == country  and
                                row['Source'] == source and
                                row['personnelsubarea'] == dag_run.conf['gender'] and
                                row['employeegroup'] == job_level,config.MAPPER))
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

        def get_mapper_timeoff_types_uri_callable():
            replicon_timeoff_types = rail.result("get_all_timeoffs")
            mapper_timeoff_types = rail.result("get_timeoff_data_from_mapper")

            return_data = list(map( lambda row:{
                    "timeoff_type_name": row['Value'],
                    "timeoff_type_uri": rail.find_first_by_attr_and_get_attr(replicon_timeoff_types,"name",row['Value'],'uri')
                }, mapper_timeoff_types))

            rail.set_result(key="unique_timeoff_uris_to_assign",
                val = list(set(map(lambda record: record['timeoff_type_uri'], filter(lambda to: bool(to['timeoff_type_uri']), return_data)))))

            rail.set_result(key="is_any_timeoff_present_in_replicon",
                val=list(filter(lambda timeoff: bool(timeoff['timeoff_type_uri']), return_data)))

            return return_data

        get_mapper_timeoff_types_uri = rail.PythonOperator(
            task_id = "get_mapper_timeoff_types_uri",
            python_callable=get_mapper_timeoff_types_uri_callable
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda: len(rail.result("get_mapper_timeoff_types_uri", key='is_any_timeoff_present_in_replicon')) > 0,
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

        get_assigned_timeoff_types = rail.PythonOperator(
            task_id = "get_assigned_timeoff_types",
            python_callable= custom_methods.get_filtered_user_timeoff_policy
        )

        is_user_rehire = rail.IfOperator(
            task_id = "is_user_rehire",
            test=lambda dag_run : dag_run.conf['rehire'].lower() == "yes",
            yes_task="trigger_rehire_timeoff_assignment_general",
            no_task="get_required_timeoff_type_details"
        )

        def get_rehire_timeoff_types_general():
            return [row for row in rail.result("get_assigned_timeoff_types") if row['policy'] and row['name'] != '[IND] Sick/ Casual leave']

        trigger_rehire_timeoff_assignment_general = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_rehire_timeoff_assignment_general",
            trigger_dag_id = config.india_rehire_user_timeoff_assignment_dag_id,
            items=get_rehire_timeoff_types_general,
            conf= lambda dag_run, item : {
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id'],
                    "user_uri":  dag_run.conf['user_uri'],
                    "loginName": dag_run.conf['loginName'],
                    "company_code": dag_run.conf['company_code'],
                    "source": dag_run.conf['source'],
                    "start_date": gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json()).strftime(gbl_custom_methods.INPUT_DATE_FORMAT),
                    "start_date_json_format": get_todays_date_for_timezone_in_json(),
                    "start_date":  (pendulum_now('America/Los_Angeles')).strftime(DATE_FORMAT),
                    "country": dag_run.conf['country'],
                    "personal_sub_area": null,
                    "employee_grp":null,
                    "employee_sub_grp": null,
                    "continuous_service_date": dag_run.conf['continuous_service_date'],
                    "continuous_service_date_json_format": dag_run.conf['continuous_service_date_json_format'],
                    "timeoff_type_uri": item['uri'],
                    "timeoff_type_name": item['name'],
                    "current_timeoff_policies": item['policy'],
                    "user_log": dag_run.conf['user_log']
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        wait_for_trigger_rehire_timeoff_assignment_general = rail.WaitForDagRunsSensor(
            task_id = "wait_for_trigger_rehire_timeoff_assignment_general",
            dag_runs="{{result('trigger_rehire_timeoff_assignment_general')}}",
            execution_timeout = timedelta(days=config.execution_timeout_days)
        )

        def get_rehire_timeoff_types_ind_sick_cas_leave():
            return [row for row in rail.result("get_assigned_timeoff_types") if row['policy'] and row['name'] == '[IND] Sick/ Casual leave']

        def get_years_based_on_dob_for_40(dag_run):
            dob_date_obj = datetime.strptime(dag_run.conf['dob'], DATE_FORMAT)
            new_date = dob_date_obj + relativedelta(years = 40)
            return new_date.strftime(DATE_FORMAT)

        trigger_rehire_timeoff_assignment_ind_sick_cas_leave = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_rehire_timeoff_assignment_ind_sick_cas_leave",
            trigger_dag_id = config.india_timeoff_assignment_ind_sick_casual_dag_id,
            items=get_rehire_timeoff_types_ind_sick_cas_leave,
            conf= lambda dag_run, item : {
                   "file_name": dag_run.conf['file_name'],
                    "user_log": dag_run.conf['user_log'],
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id'],
                    "user_uri": dag_run.conf['user_uri'],
                    "loginName": dag_run.conf['loginName'],
                    "company_code": dag_run.conf['company_code'],
                    "source": dag_run.conf['source'],
                    "start_date": dag_run.conf['start_date'],
                    "start_date_json_format": dag_run.conf['start_date_json_format'],
                    "country": dag_run.conf['country'],
                    "time_off_uri": item['uri'],
                    "time_off_name": item['name'],
                    "add": 'No',
                    "policy_set":  item['policy'],
                    "dob":dag_run.conf['dob'],
                    "dob_json_format": dag_run.conf['dob_json_format'],
                    "schedule_change_date": (pendulum_now('America/Los_Angeles')).strftime(DATE_FORMAT),
                    "tenure_based_on_dob":get_tenure_value(
                        gbl_custom_methods.convert_json_date_to_date(dag_run.conf['dob_json_format']),
                        gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json())),
                    "years_based_on_dob_for_40": get_years_based_on_dob_for_40(dag_run),
                    "rehire": "Yes"
                },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        wait_for_trigger_rehire_timeoff_assignment_ind_sick_cas_leave = rail.WaitForDagRunsSensor(
            task_id = "wait_for_trigger_rehire_timeoff_assignment_ind_sick_cas_leave",
            dag_runs="{{result('trigger_rehire_timeoff_assignment_ind_sick_cas_leave')}}",
            execution_timeout = timedelta(days=config.execution_timeout_days)
        )

        def get_required_details_callable():
            _get_mapper_timeoff_types_uri = rail.result("get_mapper_timeoff_types_uri")
            final_timeoff_list = list(filter(lambda item: bool(item['timeoff_type_uri']), _get_mapper_timeoff_types_uri))
            current_assigned_timeoffs = rail.result("get_assigned_timeoff_types")

            timeoffs_to_assign = list(filter(lambda _timeoff: _timeoff['status'] == 'No',map(
                    lambda timeoff: {
                        "name": timeoff['timeoff_type_name'],
                        "enabled":rail.find_first_by_attr_and_get_attr(current_assigned_timeoffs,'uri',timeoff['timeoff_type_uri'],'enabled'),
                        "uri": timeoff['timeoff_type_uri'],
                        "status":"Yes" if rail.find_first_by_attr_and_get_attr(current_assigned_timeoffs,'uri',timeoff['timeoff_type_uri'],'name') else "No"
                    }
                ,final_timeoff_list)))

            timeoffs_to_disable = list(filter(lambda _timeoff: _timeoff['status'] == 'No',map(lambda timeoff: {
                "name": timeoff['name'],
                "uri": timeoff['uri'],
                "enabled": timeoff['enabled'],
                "policy": timeoff['policy'],
                "status": "Yes" if rail.find_first_by_attr_and_get_attr(final_timeoff_list,"timeoff_type_uri",timeoff['uri'],'timeoff_type_name') else "No"
            }, current_assigned_timeoffs)))

            return {
                "final_time_off_list": final_timeoff_list,
                "timeoffs_to_assign": timeoffs_to_assign,
                "timeoffs_to_disable": timeoffs_to_disable
            }

        get_required_timeoff_type_details = rail.PythonOperator(
            task_id = "get_required_timeoff_type_details",
            python_callable = get_required_details_callable
        )

        def get_filtered_timeoff_types_to_disable():
            return [row for row in rail.result("get_required_timeoff_type_details")["timeoffs_to_disable"] if row['policy']]

        def get_ia_end_date_for_no_accrual(dag_run, return_type = "json"):
            _return_value = get_todays_date_for_timezone_in_json()
            if dag_run.conf['is_ia_update'].lower() == "yes":
                if dag_run.conf['is_ia'] in [1,"1"]:
                    _return_value = dag_run.conf['ia_start_date_json_format']
                else:
                    _return_value = dag_run.conf['ia_end_date_json_format']
            if return_type == "str":
                # Returning the JSON date in the downwards acceptable str format
                return f"{_return_value['year']}-{_return_value['day']}-{_return_value['month']}"
            return _return_value

        process_timeoff_no_accrual = rail.TriggerDagRunForEachItemOperator(
                task_id="process_timeoff_no_accrual",
                items=get_filtered_timeoff_types_to_disable,
                trigger_dag_id=config.india_process_time_off_no_accrual_dag_id,
                conf=lambda dag_run, item: {
                    **dag_run.conf,
                    **{
                        "timeoff_type_uri": item['uri'],
                        "policy_set": dumps(item['policy']).replace("[[{", "[{").replace("}]]", "}]"),
                        "today": get_todays_date_for_timezone_in_json(),
                        "user_end_date_json": get_ia_end_date_for_no_accrual(dag_run),
                        "end_date": get_ia_end_date_for_no_accrual(dag_run, return_type = "str")
                    }
                },
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

        wait_for_process_timeoff_no_accrual = rail.WaitForDagRunsSensor(
                task_id="wait_for_process_timeoff_no_accrual",
                dag_runs="{{result('process_timeoff_no_accrual')}}",
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run :{
                "userUri": dag_run.conf['user_uri'],
                "timeOffTypeUris": rail.result("get_mapper_timeoff_types_uri", key='unique_timeoff_uris_to_assign')
            }
        )

        has_any_timeoff_types_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_types_to_assign",
            test=lambda: bool(rail.result("get_required_timeoff_type_details")['timeoffs_to_assign']),
            yes_task="assign_timeoff_to_user",
            no_task="catch_and_log_error"
        )

        is_ia_update_yes = rail.IfOperator(
            task_id = "is_ia_update_yes",
            test=lambda dag_run: dag_run.conf['is_ia_update']=="Yes",
            yes_task="is_ia_1_and_assignment_type_host_pay",
            no_task="for_each_timeoff_types_to_assign_start_57"
        )

        is_ia_1_and_assignment_type_host_pay = rail.IfOperator(
            task_id = "is_ia_1_and_assignment_type_host_pay",
            test=lambda dag_run: dag_run.conf['is_ia'] in [1,'1'] and "host pay" in dag_run.conf['assignment_type'].lower(),
            yes_task="for_each_timeoff_types_to_assign_start_30",
            no_task="is_ia_equals_0"
        )

        for_each_timeoff_types_to_assign_start_30 = rail.ForEachOperator(
            task_id = "for_each_timeoff_types_to_assign_start_30",
            items=lambda: rail.result("get_required_timeoff_type_details")['timeoffs_to_assign'] ,
            start_task="get_default_timeoff_policy_31",
            end_task="for_each_timeoff_types_to_assign_end_30"
        )

        get_default_timeoff_policy_31 = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy_31",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_types_to_assign_start_30")['uri']
                }
            }
        )

        def get_policy_set_to_assign(dag_run, timeoff_name, default_policy):
            if default_policy:
                if  timeoff_name== "[IND] Restricted Leave":
                    if datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT).strftime("%d/%m") =="01/01":
                        return loads(dumps(default_policy).replace("null", "\"effective\"").replace("\"script\"", "\"scriptTarget\""))
                    else:
                        if datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT).strftime("%d/%m") =="01/07":
                            return loads(dumps(default_policy).replace("null", "\"effective\"").replace("\"script\"", "\"scriptTarget\""))
                        else:
                            return loads(dumps(default_policy).replace("null", "\"effective\"").replace("\"script\"", "\"scriptTarget\"").replace(
                                '''[{"keyUri":"urn:replicon:script-key:parameter:amount","value":{"number":0.0}},{"keyUri":"urn:replicon:script-key:parameter:precedence","value":{"number":10.0}}],"scriptTarget":{"description":"Set initial balance for the first day of a policy","name":"Starting Balance Set To","uri":''',
                                '''[{"keyUri":"urn:replicon:script-key:parameter:amount","value":{"number":1.0}},{"keyUri":"urn:replicon:script-key:parameter:precedence","value":{"number":10.0}}],"scriptTarget":{"description":"Set initial balance for the first day of a policy","name":"Starting Balance Set To","uri":'''))
                else:
                    return loads(dumps(default_policy).replace("null", "\"effective\"").replace("\"script\"", "\"scriptTarget\""))
            return []

        policy_set_to_assign_32 = rail.PythonOperator(
            task_id='policy_set_to_assign_32',
            python_callable=lambda dag_run: get_policy_set_to_assign(dag_run,
                rail.result("for_each_timeoff_types_to_assign_start_30")['name'], rail.result("get_default_timeoff_policy_31"))
        )

        has_any_policy_to_assign_33 = rail.IfOperator(
            task_id = "has_any_policy_to_assign_33",
            test=lambda: bool(rail.result("policy_set_to_assign_32")),
            yes_task="is_ind_sick_casual_timeoff_type_34",
            no_task="for_each_timeoff_types_to_assign_end_30"
        )

        is_ind_sick_casual_timeoff_type_34 = rail.IfOperator(
            task_id = "is_ind_sick_casual_timeoff_type_34",
            test=lambda:rail.result("for_each_timeoff_types_to_assign_start_30")['name'] == "[IND] Sick/ Casual leave",
            yes_task="trigger_timeoff_assignment_ind_sick_cas_leave_35",
            no_task="trigger_timeoff_assignment_ia_equal_1"
        )

        trigger_timeoff_assignment_ind_sick_cas_leave_35 = rail.TriggerDagRunOperator(
            task_id = "trigger_timeoff_assignment_ind_sick_cas_leave_35",
            trigger_dag_id = config.india_timeoff_assignment_ind_sick_casual_dag_id,
            conf= lambda dag_run: {
                   "file_name": dag_run.conf['file_name'],
                    "user_log": dag_run.conf['user_log'],
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id'],
                    "user_uri": dag_run.conf['user_uri'],
                    "loginName": dag_run.conf['loginName'],
                    "company_code": dag_run.conf['company_code'],
                    "source": dag_run.conf['source'],
                    "start_date": dag_run.conf['ia_start_date'],
                    "start_date_json_format": dag_run.conf['ia_start_date_json_format'],
                    "country": dag_run.conf['country'],
                    "time_off_uri": rail.result("for_each_timeoff_types_to_assign_start_30")['uri'],
                    "time_off_name": rail.result("for_each_timeoff_types_to_assign_start_30")['name'],
                    "add": 'No',
                    "policy_set":  rail.find_first_by_attr_and_get_attr(rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'], 
                        "timeOffType.uri",rail.result("for_each_timeoff_types_to_assign_start_30")['uri'], "policySetSchedule")
                        if rail.result("get_user_timeoff_policy_summary") else [],
                    "dob":dag_run.conf['dob'],
                    "dob_json_format": dag_run.conf['dob_json_format'],
                    "schedule_change_date": dag_run.conf['ia_start_date'],
                    "tenure_based_on_dob":get_tenure_value(
                        gbl_custom_methods.convert_json_date_to_date(dag_run.conf['dob_json_format']),
                        gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json())),
                    "years_based_on_dob_for_40": get_years_based_on_dob_for_40(dag_run),
                    "rehire": "No"
                },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        trigger_timeoff_assignment_ia_equal_1 = rail.TriggerDagRunOperator(
            task_id = "trigger_timeoff_assignment_ia_equal_1",
            trigger_dag_id = config.india_update_user_ia_1_timeoff_assignment_dag_id,
            conf= lambda dag_run : {
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id'],
                    "user_uri":  dag_run.conf['user_uri'],
                    "loginName": dag_run.conf['loginName'],
                    "company_code": dag_run.conf['company_code'],
                    "source": dag_run.conf['source'],
                    "start_date":  dag_run.conf['ia_start_date'],
                    "start_date_json_format": dag_run.conf['ia_start_date_json_format'],
                    "country": dag_run.conf['country'],
                    "personal_sub_area": null,
                    "employee_grp":null,
                    "employee_sub_grp": null,
                    "continuous_service_date": dag_run.conf['continuous_service_date'],
                    "continuous_service_date_json_format": dag_run.conf['continuous_service_date_json_format'],
                    "timeoff_type_uri": rail.result("for_each_timeoff_types_to_assign_start_30")['uri'],
                    "timeoff_type_name":rail.result("for_each_timeoff_types_to_assign_start_30")['name'],
                    "policy_set": rail.find_first_by_attr_and_get_attr(rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'], 
                        "timeOffType.uri",rail.result("for_each_timeoff_types_to_assign_start_30")['uri'], "policySetSchedule")
                        if rail.result("get_user_timeoff_policy_summary") else [],
                    "user_log": dag_run.conf['user_log']
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        for_each_timeoff_types_to_assign_end_30 = rail.EmptyOperator(
            task_id = "for_each_timeoff_types_to_assign_end_30"
        )


        is_ia_equals_0 = rail.IfOperator(
            task_id = "is_ia_equals_0",
            test=lambda dag_run: dag_run.conf['is_ia']in [0,'0'],
            yes_task="for_each_timeoff_types_to_assign_start_39",
            no_task="is_ia_1_and_assignment_home_pay_47"
        )

        for_each_timeoff_types_to_assign_start_39 = rail.ForEachOperator(
            task_id = "for_each_timeoff_types_to_assign_start_39",
            items=lambda: rail.result("get_required_timeoff_type_details")['timeoffs_to_assign'] ,
            start_task="get_default_timeoff_policy_40",
            end_task="for_each_timeoff_types_to_assign_end_39"
        )

        get_default_timeoff_policy_40 = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy_40",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_types_to_assign_start_39")['uri']
                }
            }
        )

        policy_set_to_assign_41 = rail.PythonOperator(
            task_id='policy_set_to_assign_41',
            python_callable=lambda dag_run: get_policy_set_to_assign(dag_run,
                rail.result("for_each_timeoff_types_to_assign_start_39")['name'], rail.result("get_default_timeoff_policy_40"))
        )

        has_any_policy_to_assign_42 = rail.IfOperator(
            task_id = "has_any_policy_to_assign_42",
            test=lambda: bool(rail.result("policy_set_to_assign_41")),
            yes_task="is_ind_sick_casual_timeoff_type_43",
            no_task="for_each_timeoff_types_to_assign_end_39"
        )

        is_ind_sick_casual_timeoff_type_43 = rail.IfOperator(
            task_id = "is_ind_sick_casual_timeoff_type_43",
            test=lambda:rail.result("for_each_timeoff_types_to_assign_start_39")['name'] == "[IND] Sick/ Casual leave",
            yes_task="trigger_timeoff_assignment_ind_sick_cas_leave_45",
            no_task="trigger_timeoff_assignment_ia_equal_0"
        )

        trigger_timeoff_assignment_ind_sick_cas_leave_45 = rail.TriggerDagRunOperator(
            task_id = "trigger_timeoff_assignment_ind_sick_cas_leave_45",
            trigger_dag_id = config.india_timeoff_assignment_ind_sick_casual_dag_id,
            conf= lambda dag_run : {
                   "file_name": dag_run.conf['file_name'],
                    "user_log": dag_run.conf['user_log'],
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id'],
                    "user_uri": dag_run.conf['user_uri'],
                    "loginName": dag_run.conf['loginName'],
                    "company_code": dag_run.conf['company_code'],
                    "source": dag_run.conf['source'],
                    "start_date": dag_run.conf['start_date'],
                    "start_date_json_format": dag_run.conf['start_date_json_format'],
                    "country": dag_run.conf['country'],
                    "time_off_uri": rail.result("for_each_timeoff_types_to_assign_start_39")['uri'],
                    "time_off_name": rail.result("for_each_timeoff_types_to_assign_start_39")['name'],
                    "add": 'No',
                    "policy_set":  rail.find_first_by_attr_and_get_attr(rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'], 
                        "timeOffType.uri",rail.result("for_each_timeoff_types_to_assign_start_39")['uri'], "policySetSchedule")
                        if rail.result("get_user_timeoff_policy_summary") else [],
                    "dob":dag_run.conf['dob'],
                    "dob_json_format": dag_run.conf['dob_json_format'],
                    "schedule_change_date": (datetime.strptime(dag_run.conf['ia_end_date'], DATE_FORMAT) + relativedelta(days=1)).strftime(DATE_FORMAT),
                    "tenure_based_on_dob":get_tenure_value(
                        gbl_custom_methods.convert_json_date_to_date(dag_run.conf['dob_json_format']),
                        gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json())),
                    "years_based_on_dob_for_40": get_years_based_on_dob_for_40(dag_run),
                    "rehire": "No"
                },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        trigger_timeoff_assignment_ia_equal_0 = rail.TriggerDagRunOperator(
            task_id = "trigger_timeoff_assignment_ia_equal_0",
            trigger_dag_id = config.india_update_user_ia_0_timeoff_assignment_dag_id,
            conf= lambda dag_run: {
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id'],
                    "user_uri":  dag_run.conf['user_uri'],
                    "loginName": dag_run.conf['loginName'],
                    "company_code": dag_run.conf['company_code'],
                    "source": dag_run.conf['source'],
                    "start_date":  dag_run.conf['start_date'],
                    "start_date_json_format": dag_run.conf['start_date_json_format'],
                    "country": dag_run.conf['country'],
                    "personal_sub_area": null,
                    "employee_grp":null,
                    "employee_sub_grp": null,
                    "continuous_service_date": dag_run.conf['continuous_service_date'],
                    "continuous_service_date_json_format": dag_run.conf['continuous_service_date_json_format'],
                    "timeoff_type_uri": rail.result("for_each_timeoff_types_to_assign_start_39")['uri'],
                    "timeoff_type_name": rail.result("for_each_timeoff_types_to_assign_start_39")['name'],
                    "policy_set": rail.find_first_by_attr_and_get_attr(rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType'], 
                        "timeOffType.uri",rail.result("for_each_timeoff_types_to_assign_start_39")['uri'], "policySetSchedule")
                        if rail.result("get_user_timeoff_policy_summary") else [],
                    "ia_end_date":dag_run.conf['ia_end_date'],
                    "ia_end_date_json_format": dag_run.conf['ia_end_date_json_format'],
                    "user_log": dag_run.conf['user_log']
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        for_each_timeoff_types_to_assign_end_39 = rail.EmptyOperator(
            task_id = "for_each_timeoff_types_to_assign_end_39"
        )

        is_ia_1_and_assignment_home_pay_47 = rail.IfOperator(
            task_id = "is_ia_1_and_assignment_home_pay_47",
            test=lambda dag_run: dag_run.conf['is_ia']in [1,'1'] and "home pay" in dag_run.conf['assignment_type'].lower(),
            yes_task="for_each_timeoff_types_to_assign_start_48",
            no_task="catch_and_log_error"
        )

        for_each_timeoff_types_to_assign_start_48 = rail.ForEachOperator(
            task_id = "for_each_timeoff_types_to_assign_start_48",
            items=lambda: rail.result("get_required_timeoff_type_details")['timeoffs_to_assign'] ,
            start_task="is_ind_sick_casual_timeoff_type_49",
            end_task="for_each_timeoff_types_to_assign_end_48"
        )

        is_ind_sick_casual_timeoff_type_49 = rail.IfOperator(
            task_id = "is_ind_sick_casual_timeoff_type_49",
            test=lambda:rail.result("for_each_timeoff_types_to_assign_start_48")['name'] == "[IND] Sick/ Casual leave",
            yes_task="trigger_timeoff_assignment_ind_sick_cas_leave_50",
            no_task="get_default_timeoff_policy_52"
        )

        trigger_timeoff_assignment_ind_sick_cas_leave_50 = rail.TriggerDagRunOperator(
            task_id = "trigger_timeoff_assignment_ind_sick_cas_leave_50",
            trigger_dag_id = config.india_timeoff_assignment_ind_sick_casual_dag_id,
            conf= lambda dag_run: {
                    "file_name": dag_run.conf['file_name'],
                    "user_log": dag_run.conf['user_log'],
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id'],
                    "user_uri": dag_run.conf['user_uri'],
                    "loginName": dag_run.conf['loginName'],
                    "company_code": dag_run.conf['company_code'],
                    "source": dag_run.conf['source'],
                    "start_date": dag_run.conf['start_date'],
                    "start_date_json_format": dag_run.conf['start_date_json_format'],
                    "country": dag_run.conf['country'],
                    "time_off_uri": rail.result("for_each_timeoff_types_to_assign_start_48")['uri'],
                    "time_off_name": rail.result("for_each_timeoff_types_to_assign_start_48")['name'],
                    "add": 'Yes',
                    "policy_set":  null,
                    "dob":dag_run.conf['dob'],
                    "dob_json_format": dag_run.conf['dob_json_format'],
                    "schedule_change_date": null,
                    "tenure_based_on_dob":get_tenure_value(
                        gbl_custom_methods.convert_json_date_to_date(dag_run.conf['dob_json_format']),
                        gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json())),
                    "years_based_on_dob_for_40": get_years_based_on_dob_for_40(dag_run),
                    "rehire": "No"
                },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        get_default_timeoff_policy_52 = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy_52",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_types_to_assign_start_48")['uri']
                }
            }
        )

        policy_set_to_assign_53= rail.PythonOperator(
            task_id='policy_set_to_assign_53',
            python_callable=lambda dag_run: get_policy_set_to_assign(dag_run,
                rail.result("for_each_timeoff_types_to_assign_start_48")['name'], rail.result("get_default_timeoff_policy_52"))
        )

        has_any_policy_to_assign_54 = rail.IfOperator(
            task_id = "has_any_policy_to_assign_54",
            test=lambda: bool(rail.result("policy_set_to_assign_53")),
            yes_task="put_user_timeoff_policy_set_55",
            no_task="for_each_timeoff_types_to_assign_end_48"
        )

        put_user_timeoff_policy_set_55 = rail.RepliconServiceOperator(
            task_id = "put_user_timeoff_policy_set_55",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_types_to_assign_start_48")['uri']
                },
                "policySetScheduleEntries": rail.result('policy_set_to_assign_53')
            }
        )

        for_each_timeoff_types_to_assign_end_48 = rail.EmptyOperator(
            task_id = "for_each_timeoff_types_to_assign_end_48"
        )


        for_each_timeoff_types_to_assign_start_57 = rail.ForEachOperator(
            task_id = "for_each_timeoff_types_to_assign_start_57",
            items=lambda: rail.result("get_required_timeoff_type_details")['timeoffs_to_assign'] ,
            start_task="is_ind_sick_casual_timeoff_type_58",
            end_task="for_each_timeoff_types_to_assign_end_57"
        )

        is_ind_sick_casual_timeoff_type_58 = rail.IfOperator(
            task_id = "is_ind_sick_casual_timeoff_type_58",
            test=lambda:rail.result("for_each_timeoff_types_to_assign_start_57")['name'] == "[IND] Sick/ Casual leave",
            yes_task="trigger_timeoff_assignment_ind_sick_cas_leave_59",
            no_task="get_default_timeoff_policy_61"
        )

        trigger_timeoff_assignment_ind_sick_cas_leave_59 = rail.TriggerDagRunOperator(
            task_id = "trigger_timeoff_assignment_ind_sick_cas_leave_59",
            trigger_dag_id = config.india_timeoff_assignment_ind_sick_casual_dag_id,
            conf= lambda dag_run: {
                    "file_name": dag_run.conf['file_name'],
                    "user_log": dag_run.conf['user_log'],
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id'],
                    "user_uri": dag_run.conf['user_uri'],
                    "loginName": dag_run.conf['loginName'],
                    "company_code": dag_run.conf['company_code'],
                    "source": dag_run.conf['source'],
                    "start_date": dag_run.conf['start_date'],
                    "start_date_json_format": dag_run.conf['start_date_json_format'],
                    "country": dag_run.conf['country'],
                    "time_off_uri": rail.result("for_each_timeoff_types_to_assign_start_57")['uri'],
                    "time_off_name": rail.result("for_each_timeoff_types_to_assign_start_57")['name'],
                    "add": 'Yes',
                    "policy_set":  null,
                    "dob":dag_run.conf['dob'],
                    "dob_json_format": dag_run.conf['dob_json_format'],
                    "schedule_change_date": null,
                    "tenure_based_on_dob":get_tenure_value(
                        gbl_custom_methods.convert_json_date_to_date(dag_run.conf['dob_json_format']),
                        gbl_custom_methods.convert_json_date_to_date(get_todays_date_for_timezone_in_json())),
                    "years_based_on_dob_for_40": get_years_based_on_dob_for_40(dag_run),
                    "rehire": "No"
                },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        get_default_timeoff_policy_61 = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy_61",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_types_to_assign_start_57")['uri']
                }
            }
        )

        policy_set_to_assign_62= rail.PythonOperator(
            task_id='policy_set_to_assign_62',
            python_callable=lambda dag_run: get_policy_set_to_assign(dag_run,
                rail.result("for_each_timeoff_types_to_assign_start_57")['name'], rail.result("get_default_timeoff_policy_61"))
        )

        has_any_policy_to_assign_63 = rail.IfOperator(
            task_id = "has_any_policy_to_assign_63",
            test=lambda: bool(rail.result("policy_set_to_assign_62")),
            yes_task="put_user_timeoff_policy_set_64",
            no_task="for_each_timeoff_types_to_assign_end_57"
        )

        put_user_timeoff_policy_set_64 = rail.RepliconServiceOperator(
            task_id = "put_user_timeoff_policy_set_64",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_types_to_assign_start_57")['uri']
                },
                "policySetScheduleEntries": rail.result('policy_set_to_assign_62')
            }
        )

        for_each_timeoff_types_to_assign_end_57 = rail.EmptyOperator(
            task_id = "for_each_timeoff_types_to_assign_end_57"
        )


        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Update",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid":  "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Update',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_timeoff_data_from_mapper

        get_timeoff_data_from_mapper >> has_any_data >> rail.Label('Yes') >> get_all_timeoffs
        has_any_data >> rail.Label('No') >> catch_and_log_error

        get_all_timeoffs >> get_mapper_timeoff_types_uri >> has_any_timeoff_to_assign
        has_any_timeoff_to_assign >> rail.Label('No') >> catch_and_log_error
        has_any_timeoff_to_assign >> rail.Label('Yes') >> get_user_timeoff_policy_summary >> get_assigned_timeoff_types
        get_assigned_timeoff_types >> is_user_rehire

        is_user_rehire >> rail.Label('No') >> get_required_timeoff_type_details 
        is_user_rehire >> rail.Label('Yes') >> trigger_rehire_timeoff_assignment_general >> wait_for_trigger_rehire_timeoff_assignment_general
        wait_for_trigger_rehire_timeoff_assignment_general >> trigger_rehire_timeoff_assignment_ind_sick_cas_leave
        trigger_rehire_timeoff_assignment_ind_sick_cas_leave >> wait_for_trigger_rehire_timeoff_assignment_ind_sick_cas_leave
        wait_for_trigger_rehire_timeoff_assignment_ind_sick_cas_leave >> get_required_timeoff_type_details >> process_timeoff_no_accrual
        process_timeoff_no_accrual >> wait_for_process_timeoff_no_accrual >> has_any_timeoff_types_to_assign
        
        has_any_timeoff_types_to_assign >> rail.Label('Yes') >> assign_timeoff_to_user >> is_ia_update_yes
        has_any_timeoff_types_to_assign >> rail.Label('No') >> catch_and_log_error

        is_ia_update_yes >> rail.Label('No') >> for_each_timeoff_types_to_assign_start_57
        is_ia_update_yes >> rail.Label('Yes') >> is_ia_1_and_assignment_type_host_pay >> rail.Label('No') >> is_ia_equals_0
        is_ia_1_and_assignment_type_host_pay >> rail.Label('Yes') >> for_each_timeoff_types_to_assign_start_30
        for_each_timeoff_types_to_assign_start_30 >> for_each_timeoff_types_to_assign_end_30
        for_each_timeoff_types_to_assign_start_30 >> get_default_timeoff_policy_31 >> policy_set_to_assign_32 >> has_any_policy_to_assign_33
        has_any_policy_to_assign_33 >> rail.Label('Yes') >> is_ind_sick_casual_timeoff_type_34
        has_any_policy_to_assign_33 >> rail.Label('No') >> for_each_timeoff_types_to_assign_end_30

        is_ind_sick_casual_timeoff_type_34 >> rail.Label('No') >> trigger_timeoff_assignment_ia_equal_1 >> for_each_timeoff_types_to_assign_end_30
        is_ind_sick_casual_timeoff_type_34 >> rail.Label('Yes') >> trigger_timeoff_assignment_ind_sick_cas_leave_35 >> for_each_timeoff_types_to_assign_end_30

        for_each_timeoff_types_to_assign_end_30 >> is_ia_equals_0

        is_ia_equals_0 >> rail.Label('Yes') >> for_each_timeoff_types_to_assign_start_39
        is_ia_equals_0 >> rail.Label('No') >> is_ia_1_and_assignment_home_pay_47

        for_each_timeoff_types_to_assign_start_39 >> get_default_timeoff_policy_40 >> policy_set_to_assign_41
        policy_set_to_assign_41 >> has_any_policy_to_assign_42 >> rail.Label('Yes') >> is_ind_sick_casual_timeoff_type_43
        has_any_policy_to_assign_42 >> rail.Label('No') >> for_each_timeoff_types_to_assign_end_39

        is_ind_sick_casual_timeoff_type_43 >> rail.Label('Yes') >> trigger_timeoff_assignment_ind_sick_cas_leave_45 >> for_each_timeoff_types_to_assign_end_39
        is_ind_sick_casual_timeoff_type_43 >> rail.Label('No') >> trigger_timeoff_assignment_ia_equal_0 >> for_each_timeoff_types_to_assign_end_39

        for_each_timeoff_types_to_assign_start_39 >> for_each_timeoff_types_to_assign_end_39

        for_each_timeoff_types_to_assign_end_39 >> is_ia_1_and_assignment_home_pay_47 >> rail.Label('No') >> catch_and_log_error
        is_ia_1_and_assignment_home_pay_47 >> rail.Label('Yes') >>  for_each_timeoff_types_to_assign_start_48

        for_each_timeoff_types_to_assign_start_48 >> is_ind_sick_casual_timeoff_type_49
        is_ind_sick_casual_timeoff_type_49 >> rail.Label("Yes") >> trigger_timeoff_assignment_ind_sick_cas_leave_50 >> for_each_timeoff_types_to_assign_end_48
        is_ind_sick_casual_timeoff_type_49 >> rail.Label("No") >> get_default_timeoff_policy_52 >> policy_set_to_assign_53
        policy_set_to_assign_53 >> has_any_policy_to_assign_54 >> rail.Label('No') >> for_each_timeoff_types_to_assign_end_48
        has_any_policy_to_assign_54 >> rail.Label('Yes') >> put_user_timeoff_policy_set_55 >> for_each_timeoff_types_to_assign_end_48

        for_each_timeoff_types_to_assign_start_48 >> for_each_timeoff_types_to_assign_end_48
        for_each_timeoff_types_to_assign_end_48 >> catch_and_log_error

        for_each_timeoff_types_to_assign_start_57 >> for_each_timeoff_types_to_assign_end_57

        for_each_timeoff_types_to_assign_start_57 >> is_ind_sick_casual_timeoff_type_58 >> rail.Label('No') >> get_default_timeoff_policy_61
        is_ind_sick_casual_timeoff_type_58 >> rail.Label('Yes') >>  trigger_timeoff_assignment_ind_sick_cas_leave_59
        trigger_timeoff_assignment_ind_sick_cas_leave_59 >> for_each_timeoff_types_to_assign_end_57
        get_default_timeoff_policy_61 >> policy_set_to_assign_62 >> has_any_policy_to_assign_63
        has_any_policy_to_assign_63 >> rail.Label('Yes') >> put_user_timeoff_policy_set_64 >> for_each_timeoff_types_to_assign_end_57
        has_any_policy_to_assign_63 >> rail.Label('No') >> for_each_timeoff_types_to_assign_end_57 >> catch_and_log_error

        return dag

rail.for_each_instance(create_update_user_timeoff_assignment_dag)
