from json import dumps, loads
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from datetime import timedelta

def create_add_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.costa_rica_add_user_timeoff_assignment_dag_id,
        description="DXC Workday User Import Costa Rica - Process Add User TimeOff Assignment",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_add_user_timeoff_assignemnt_costa_rica
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_costa_rica, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="assign_timeoff_to_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="assign_timeoff_to_user",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUris": dag_run.conf['timeoffs_to_assign_uri_list']
                }
        )

        for_each_default_timeoff_start = rail.ForEachOperator(
            task_id = "for_each_default_timeoff_start",
            items=lambda dag_run: dag_run.conf["formatted_timeoff_to_assign_uri_list"],
            start_task="get_default_timeoff_policy",
            end_task="for_each_default_timeoff_end"
        )

        get_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run:{
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_default_timeoff_start")['timeoff_uri']
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
            no_task= "for_each_default_timeoff_end"
        )

        put_user_timeoff_policy_set = rail.RepliconServiceOperator(
            task_id = "put_user_timeoff_policy_set",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_default_timeoff_start")['timeoff_uri']
                },
                "policySetScheduleEntries": rail.result('policy_set_to_assign')
            }
        )

        for_each_default_timeoff_end = rail.EmptyOperator(
            task_id = "for_each_default_timeoff_end"
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = lambda dag_run: dag_run.conf['user_log'],
            trigger_rule = "one_failed",
            message="User Add",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Add',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> assign_timeoff_to_user

        assign_timeoff_to_user >> for_each_default_timeoff_start

        for_each_default_timeoff_start >> get_default_timeoff_policy >> policy_set_to_assign >> has_any_policy_to_assign
        has_any_policy_to_assign >> rail.Label('Yes') >> put_user_timeoff_policy_set
        has_any_policy_to_assign >> rail.Label('No') >> for_each_default_timeoff_end
        put_user_timeoff_policy_set >> for_each_default_timeoff_end
        for_each_default_timeoff_start >> for_each_default_timeoff_end >> catch_and_log_error

        return dag

rail.for_each_instance(create_add_user_timeoff_assignment_dag)
