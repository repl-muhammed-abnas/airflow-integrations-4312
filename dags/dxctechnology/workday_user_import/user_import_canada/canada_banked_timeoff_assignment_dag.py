from datetime import timedelta
from functools import lru_cache
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import convert_json_date_to_date


def create_update_user_canada_bank_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_canada_users_process_canada_banked_timeoff_type_child_dag,
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
            no_task="get_default_policies_for_timeoff_type",
            yes_task="batch_task"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_default_policies_for_timeoff_type",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )
        
        get_default_policies_for_timeoff_type = rail.RepliconServiceOperator(
            task_id = "get_default_policies_for_timeoff_type",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data= {
                "timeOffTypeUri": "{{ dag_run.conf.secondary_timeoff_policy.uri }}"
            }
        )


        def prepare_payload_callable(dag_run):

            policy_to_assign = []

            policy_sets = dag_run.conf['policy_sets']
            start_date_json = dag_run.conf['start_date_json']

            default_policy_set_for_timeoff = rail.result("get_default_policies_for_timeoff_type")

            for _policy in policy_sets:
                if _policy['effectiveDate'] and _policy['effectiveDate']['day']:
                    if convert_json_date_to_date(_policy['effectiveDate']) < convert_json_date_to_date(start_date_json):
                        policy_to_assign.append({
                            "description": _policy['description'],
                            "effectiveDate": _policy['effectiveDate'],
                            "policySet": _policy['policySet']
                        })

            policy_to_assign.append({
                            "description": f"Effective on {start_date_json['day']}/{start_date_json['month']}/{start_date_json['year']}",
                            "effectiveDate": {
                                "day": start_date_json['day'],
                                "month": start_date_json['month'],
                                "year": start_date_json['year']
                            },
                            "policySet": default_policy_set_for_timeoff[0]['policySet']
                        })

            return loads(dumps(policy_to_assign).replace("\"script\"", "\"scriptTarget\""))

        prepare_payload = rail.PythonOperator(
            task_id = "prepare_payload",
            python_callable= prepare_payload_callable
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test = lambda : bool(rail.result("prepare_payload")),
            yes_task = "assign_timeoff_policies",
            no_task = "catch_and_log_error"
        )

        assign_timeoff_policies = rail.RepliconServiceOperator(
            task_id = "assign_timeoff_policies",
            endpoint = "/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run : {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri']
                },
                "policySetScheduleEntries": rail.result("prepare_payload")
            }
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
                "Email": dag_run.conf["login_name"],
                "Action": 'Update',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("No") >> get_default_policies_for_timeoff_type
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error

        get_default_policies_for_timeoff_type >> prepare_payload >> has_any_policy_to_assign >> rail.Label("Yes") >> assign_timeoff_policies >> catch_and_log_error
        has_any_policy_to_assign >> rail.Label("No") >> catch_and_log_error

        return dag

rail.for_each_instance(create_update_user_canada_bank_timeoff_assignment_dag)
