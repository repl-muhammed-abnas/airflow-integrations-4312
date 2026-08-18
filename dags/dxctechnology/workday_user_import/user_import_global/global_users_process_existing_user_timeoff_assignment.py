from datetime import timedelta
from functools import lru_cache
from json import dumps
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import.user_import_global.utils.request_payload import get_update_timeoff_policies_payload_update_user, get_timeoff_assignment_payload_for_update_user
from dxctechnology.workday_user_import.user_import_global.utils.custom_methods import map_mapper_replicon_timeoffs_update_user
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import get_date_to_use_for_no_accrual

# Non- Canada
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_global_users_update_user_timeoff_process_child_dag,
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
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
            python_callable=lambda dag_run: list(filter(lambda row: row['Type']=='Timeoff' and\
                                                                    row['Country']=='ALL' and\
                                                                    row['Function']=='Workday User Sync' and\
                                                                    row['Source'] == dag_run.conf['parent_company_code'], config.MAPPER))
        )

        map_mapper_replicon_timeoff = rail.PythonOperator(
            task_id = "map_mapper_replicon_timeoff",
            python_callable=map_mapper_replicon_timeoffs_update_user
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda: len(rail.result("map_mapper_replicon_timeoff")) > 0,
            yes_task="get_user_timeoff_policy_summary"
        )

        get_user_timeoff_policy_summary = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri" : "{{dag_run.conf.user_uri}}"
            }
        )

        def get_timeoff_to_disable_and_to_assign_callable():
            current_timeoffs_policies = list(map(lambda timeoff : {
                    "name": timeoff['timeOffType']["name"],
                    "enabled": timeoff["isTimeOffAllowedAgainstThisTimeOffType"],
                    "uri":timeoff["timeOffType"]['uri'],
                    "policy":timeoff["policySetSchedule"]
                },rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType']))
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
                "status": "Yes" if rail.find_first_by_attr_and_get_attr(mapped_timeoff_data, 'uri', to2['uri'], 'name') else "No",
                "policy": to2['policy']
            }, current_timeoffs_policies)))

            rail.set_result(key="timeoff_to_disable",val=timeoff_to_disable)

            return timeoff_to_assign

        get_timeoff_to_disable_and_to_assign = rail.PythonOperator(
            task_id = "get_timeoff_to_disable_and_to_assign",
            python_callable=get_timeoff_to_disable_and_to_assign_callable
        )

        is_ia_updated = rail.IfOperator(
            task_id = "is_ia_updated",
            test = lambda dag_run: dag_run.conf['ia_updated'] in [True, 'true', 'True'],
            yes_task = "process_no_accrual",
            no_task = "has_any_timeoff_to_assign_to_user"
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
                        "hire_date": dag_run.conf['hire_date'],
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

        has_any_timeoff_to_assign_to_user = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign_to_user",
            test="{{result('get_timeoff_to_disable_and_to_assign') | is_truthy}}",
            yes_task="assign_timeoff_to_user"
        )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=get_timeoff_assignment_payload_for_update_user
        )

        # process_normal_timeoffs in for loop
        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items=lambda: [timeoff for timeoff in rail.result("get_timeoff_to_disable_and_to_assign") if timeoff['uri']],
            start_task="is_ia_updated_2",
            end_task="empty_process_timeoff"
        )

        is_ia_updated_2 = rail.IfOperator(
            task_id = "is_ia_updated_2",
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

        get_all_timeoffs >> query_timeoff_data >> map_mapper_replicon_timeoff >> has_any_timeoff_to_assign >> rail.Label(
            "Yes") >> get_user_timeoff_policy_summary >> get_timeoff_to_disable_and_to_assign >> is_ia_updated >> rail.Label("No") >> has_any_timeoff_to_assign_to_user >> rail.Label(
            "Yes") >> assign_timeoff_to_user >> for_each_timeoff
        is_ia_updated >> rail.Label("Yes") >> process_no_accrual >> wait_for_no_accrual >> has_any_timeoff_to_assign_to_user

        for_each_timeoff >> is_ia_updated_2 >> rail.Label("No") >> get_default_timeoff_policy >> has_any_policies >> rail.Label("Yes") >> update_timeoff_policies >> empty_process_timeoff
        is_ia_updated_2 >> rail.Label("Yes") >> is_ia_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment >> add_dag_run_id_for_wait >> empty_process_timeoff
        is_ia_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> add_dag_run_id_for_wait2 >> empty_process_timeoff
        has_any_policies >> rail.Label("Yes") >> empty_process_timeoff >> catch_and_log_error
        for_each_timeoff >> empty_process_timeoff

        return dag

rail.for_each_instance(create_dag)

