from datetime import timedelta
from json import dumps, loads
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import get_date_to_use_for_no_accrual
from dxctechnology.workday_user_import.user_import.common_utils import request_payload as gbl_request_payload
from dxctechnology.workday_user_import.user_import_costa_rica.utils import custom_methods
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_json_date_from_date_str
from dxctechnology.workday_user_import.user_import_costa_rica.mappers.costa_rica_specific_timeoffs import should_preserve_timeoff, get_preserved_timeoff_names
null = None

# pylint: disable=too-many-statements
def create_update_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id = config.costa_rica_update_user_timeoff_assignment_dag_id,
        description = "DXC Workday User Import Costa Rica - Process Update User TimeOff Assignment",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.max_active_run_update_user_timeoff_assignment_costa_rica
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_costa_rica, default_var='true').lower() == 'true',
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
            work_shift = dag_run.conf['workshift']
            exempt = dag_run.conf['exempt']
            time_off_data = list(filter(lambda row:  row['Type'] == "Timeoff" and
                                row['Function'] == "Workday User Sync" and
                                row['Country'] == country  and
                                row['Source'] == source and
                                row['personnelsubarea'] == exempt  and
                                row['employeegroup'] == work_shift,config.MAPPER))
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

        def timeoff_to_assign():
            replicon_timeoffs = rail.result("get_all_timeoffs")
            mapper_timeoffs = rail.result('get_timeoff_data_from_mapper')

            timeoff_list =  list(map(lambda timeoff: {
                    "name": timeoff['Value'],
                    "uri": rail.find_first_by_attr_and_get_attr(
                        replicon_timeoffs, 'name', timeoff['Value'].strip(), 'uri'),
                    "policy_type": timeoff['URI'] if timeoff['URI'] else null
                }, mapper_timeoffs))
            
            filtered_timeoff_list = list(filter(lambda x: bool(x['uri']), timeoff_list))
            
            timeoff_unique_uri_list_to_assign = list(set(map(lambda record: record['uri'], filtered_timeoff_list)))

            rail.set_result(key = "timeoff_list_mapped_as_per_replicon", val = timeoff_list)

            rail.set_result(key = "timeoff_list_to_assign", val = filtered_timeoff_list )

            rail.set_result(key = "formatted_timeoff_uri_list_to_assign", val = [{"timeoff_uri": item } for item in timeoff_unique_uri_list_to_assign])

            return timeoff_unique_uri_list_to_assign

        get_mapper_timeoff_types_uri = rail.PythonOperator(
            task_id = "get_mapper_timeoff_types_uri",
            python_callable=timeoff_to_assign
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda: len(rail.result("get_mapper_timeoff_types_uri")) > 0,
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
            python_callable= lambda: custom_methods.get_filtered_user_timeoff_policy(rail.result("get_user_timeoff_policy_summary"))
        )

        is_user_rehire = rail.IfOperator(
            task_id = "is_user_rehire",
            test=lambda dag_run : dag_run.conf['rehire'].lower() == "yes",
            yes_task="trigger_rehire_timeoff_assignment",
            no_task="get_required_timeoff_type_details"
        )

        def get_rehire_timeoff_types():
            return [row for row in rail.result("get_assigned_timeoff_types") if row['policy']]

        trigger_rehire_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_rehire_timeoff_assignment",
            trigger_dag_id = config.costa_rica_rehire_user_timeoff_assignment_dag_id,
            items=get_rehire_timeoff_types,
            conf= lambda dag_run, item : {
                    "emp_id": dag_run.conf['emp_id'],
                    "email_id": dag_run.conf['email_id'],
                    "user_uri":  dag_run.conf['user_uri'],
                    "loginName": dag_run.conf['loginName'],
                    "company_code": dag_run.conf['company_code'],
                    "source": dag_run.conf['source'],
                    "start_date": dag_run.conf['start_date'],
                    "start_date_json_format": dag_run.conf['start_date_json_format'],
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

        wait_for_trigger_rehire_timeoff_assignment = rail.WaitForDagRunsSensor(
            task_id = "wait_for_trigger_rehire_timeoff_assignment",
            dag_runs="{{result('trigger_rehire_timeoff_assignment')}}",
            execution_timeout = timedelta(days=config.execution_timeout_days)
        )

        def get_required_details_callable(dag_run):
            final_timeoff_list = rail.result("get_mapper_timeoff_types_uri", key="formatted_timeoff_uri_list_to_assign")
            timeoff_list_as_per_mapper = rail.result("get_mapper_timeoff_types_uri", key="timeoff_list_mapped_as_per_replicon")
            current_assigned_timeoffs = rail.result("get_assigned_timeoff_types")

            timeoffs_to_assign = list(filter(lambda _timeoff: _timeoff['status'] == 'No',map(
                    lambda timeoff: {
                        "name": rail.find_first_by_attr_and_get_attr(timeoff_list_as_per_mapper,'uri',timeoff['timeoff_uri'],'name'),
                        "enabled":rail.find_first_by_attr_and_get_attr(current_assigned_timeoffs,'uri',timeoff['timeoff_uri'],'enabled'),
                        "uri": timeoff['timeoff_uri'],
                        "status":"Yes" if rail.find_first_by_attr_and_get_attr(current_assigned_timeoffs,'uri',timeoff['timeoff_uri'],'name') else "No"
                    }
                ,final_timeoff_list)))

            timeoffs_to_disable = list(filter(lambda _timeoff: _timeoff['status'] == 'No',map(lambda timeoff: {
                "name": timeoff['name'],
                "uri": timeoff['uri'],
                "enabled": timeoff['enabled'],
                "policy": timeoff['policy'],
                "status": "Yes" if rail.find_first_by_attr_and_get_attr(final_timeoff_list,"timeoff_uri",timeoff['uri']) else "No"
            }, current_assigned_timeoffs)))

            # Retention logic: Preserve [CR] Senority Vacation day for eligible users
            # Filter out seniority vacation from disable list if user is eligible
            seniority_uri_to_preserve = None
            if custom_methods.should_preserve_seniority_vacation(dag_run, current_assigned_timeoffs, "update"):
                timeoffs_to_disable = list(filter(
                    lambda timeoff: not should_preserve_timeoff(timeoff['name']),
                    timeoffs_to_disable
                ))
                # Get the seniority vacation URI so it can be included in the assign list
                preserved_names = get_preserved_timeoff_names()
                for timeoff in current_assigned_timeoffs:
                    if isinstance(timeoff, dict) and timeoff.get('name') in preserved_names:
                        seniority_uri_to_preserve = timeoff['uri']
                        break

            rail.set_result(key="seniority_uri_to_preserve", val=seniority_uri_to_preserve)

            return {
                "final_time_off_list": final_timeoff_list,
                "timeoffs_to_assign": timeoffs_to_assign,
                "timeoffs_to_disable": timeoffs_to_disable
            }

        get_required_timeoff_type_details = rail.PythonOperator(
            task_id = "get_required_timeoff_type_details",
            python_callable = get_required_details_callable
        )

        has_any_timeoff_to_disable = rail.IfOperator(
            task_id = "has_any_timeoff_to_disable",
            test=lambda : len(rail.result('get_required_timeoff_type_details')['timeoffs_to_disable']) > 0,
            yes_task="process_timeoff_no_accrual",
            no_task="has_any_timeoff_types_to_assign"
        )

        def get_filtered_timeoff_types_to_disable():
            return [row for row in rail.result("get_required_timeoff_type_details")["timeoffs_to_disable"] if row['policy']]

        process_timeoff_no_accrual = rail.TriggerDagRunForEachItemOperator(
                task_id="process_timeoff_no_accrual",
                items=get_filtered_timeoff_types_to_disable,
                trigger_dag_id=config.costa_rica_process_time_off_no_accrual_dag_id,
                conf=lambda dag_run, item: {
                    **dag_run.conf,
                    **{
                        "timeoff_type_uri": item['uri'],
                        "policy_set": dumps(item['policy']).replace("[[{", "[{").replace("}]]", "}]"),
                        "today": gbl_request_payload.get_todays_date_for_timezone_in_json(),
                        "user_end_date_json": get_date_to_use_for_no_accrual(dag_run, {})
                    }
                },
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

        wait_for_process_timeoff_no_accrual = rail.WaitForDagRunsSensor(
                task_id="wait_for_process_timeoff_no_accrual",
                dag_runs="{{result('process_timeoff_no_accrual')}}",
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

        has_any_timeoff_types_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_types_to_assign",
            test=lambda: bool(rail.result("get_required_timeoff_type_details")['timeoffs_to_assign']),
            yes_task="assign_timeoff_to_user",
            no_task="catch_and_log_error"
        )

        def get_timeoff_uris_for_assignment():
            mapper_uris = rail.result("get_mapper_timeoff_types_uri")
            seniority_uri = rail.result("get_required_timeoff_type_details", key="seniority_uri_to_preserve")
            if seniority_uri and seniority_uri not in mapper_uris:
                return mapper_uris + [seniority_uri]
            return mapper_uris

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run :{
                "userUri": dag_run.conf['user_uri'],
                "timeOffTypeUris": get_timeoff_uris_for_assignment()
            }
        )


        for_each_timeoff_types_to_assign_start = rail.ForEachOperator(
            task_id = "for_each_timeoff_types_to_assign_start",
            items=lambda: rail.result("get_required_timeoff_type_details")['timeoffs_to_assign'] ,
            start_task="is_ia_updated",
            end_task="for_each_timeoff_types_to_assign_end"
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
                "source": dag_run.conf.get('source', ''),
                "star_date": dag_run.conf['ia_start_date'],
                "country": dag_run.conf['country'],
                "personnel_subarea": "",
                "employee_group":"",
                "employee_subgroup": "",
                "contineous_service_date": dag_run.conf['start_date'],
                "timeoff_uri": rail.result("for_each_timeoff_types_to_assign_start")['uri'],
                "timeoff_name": rail.result("for_each_timeoff_types_to_assign_start")['name'],
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
                "source": dag_run.conf.get('source', ''),
                "star_date": dag_run.conf['start_date'],
                "ia_end_date": dag_run.conf['ia_end_date'],
                "country": dag_run.conf['country'],
                "personnel_subarea": "",
                "employee_group":"",
                "employee_subgroup": "",
                "contineous_service_date": dag_run.conf['start_date'],
                "timeoff_uri": rail.result("for_each_timeoff_types_to_assign_start")['uri'],
                "timeoff_name": rail.result("for_each_timeoff_types_to_assign_start")['name'],
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

        add_dag_run_id_for_wait2 = rail.PythonOperator(
            task_id = "add_dag_run_id_for_wait2",
            python_callable=lambda: (rail.result("add_dag_run_id_for_wait2") + [rail.result("trigger_ia_zero_timeoff_assignment")]
                                     ) if rail.result("add_dag_run_id_for_wait2") else [rail.result("trigger_ia_zero_timeoff_assignment")]
        )


        get_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_types_to_assign_start")['uri']
                }
            }
        )

        policy_set_to_assign = rail.PythonOperator(
            task_id='policy_set_to_assign',
            python_callable=lambda: loads(dumps(rail.result("get_default_timeoff_policy")
                            ).replace("null", "\"effective\""
                        ).replace("\"script\"", "\"scriptTarget\""
                        )) if rail.result("get_default_timeoff_policy") else []
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test=lambda: bool(rail.result("policy_set_to_assign")),
            yes_task="put_user_timeoff_policy_set",
            no_task="for_each_timeoff_types_to_assign_end"
        )

        put_user_timeoff_policy_set = rail.RepliconServiceOperator(
            task_id = "put_user_timeoff_policy_set",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_types_to_assign_start")['uri']
                },
                "policySetScheduleEntries": rail.result('policy_set_to_assign')
            }
        )

        for_each_timeoff_types_to_assign_end = rail.EmptyOperator(
            task_id = "for_each_timeoff_types_to_assign_end"
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

        get_timeoff_data_from_mapper >> has_any_data >> rail.Label("No") >> catch_and_log_error
        has_any_data >> rail.Label("Yes") >> get_all_timeoffs >> get_mapper_timeoff_types_uri >> has_any_timeoff_to_assign
        has_any_timeoff_to_assign >> rail.Label("No") >> catch_and_log_error
        has_any_timeoff_to_assign >> rail.Label("Yes") >> get_user_timeoff_policy_summary >> get_assigned_timeoff_types
        get_assigned_timeoff_types >> is_user_rehire >> rail.Label("Yes") >> trigger_rehire_timeoff_assignment
        trigger_rehire_timeoff_assignment >> wait_for_trigger_rehire_timeoff_assignment >> get_required_timeoff_type_details
        is_user_rehire >> rail.Label("No") >> get_required_timeoff_type_details >> has_any_timeoff_to_disable
        has_any_timeoff_to_disable >> rail.Label("Yes") >> process_timeoff_no_accrual
        has_any_timeoff_to_disable >> rail.Label("No") >> has_any_timeoff_types_to_assign
        process_timeoff_no_accrual >> wait_for_process_timeoff_no_accrual
        wait_for_process_timeoff_no_accrual >> has_any_timeoff_types_to_assign

        has_any_timeoff_types_to_assign >> rail.Label("No") >> catch_and_log_error
        has_any_timeoff_types_to_assign >> rail.Label("Yes") >> assign_timeoff_to_user >> for_each_timeoff_types_to_assign_start

        for_each_timeoff_types_to_assign_start >>  is_ia_updated >>  rail.Label("No")>> get_default_timeoff_policy >> policy_set_to_assign >> has_any_policy_to_assign

        is_ia_updated >> rail.Label("Yes") >> is_ia_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment >> add_dag_run_id_for_wait >> for_each_timeoff_types_to_assign_end
        is_ia_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> add_dag_run_id_for_wait2 >> for_each_timeoff_types_to_assign_end

        has_any_policy_to_assign >> rail.Label("No") >> for_each_timeoff_types_to_assign_end
        has_any_policy_to_assign >> rail.Label("Yes") >> put_user_timeoff_policy_set >> for_each_timeoff_types_to_assign_end

        for_each_timeoff_types_to_assign_start >> for_each_timeoff_types_to_assign_end >> gather_runids_for_wait >> wait_for_user_timeoff_update_completion >> catch_and_log_error

        return dag

rail.for_each_instance(create_update_user_timeoff_assignment_dag)
