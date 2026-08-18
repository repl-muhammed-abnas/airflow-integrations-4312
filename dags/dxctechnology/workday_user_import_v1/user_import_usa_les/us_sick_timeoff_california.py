from datetime import timedelta
from dateutil.relativedelta import relativedelta
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import.common_utils \
    import custom_methods as common_custom_methods


def create_us_sick_timeoff_california_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.usa_les_us_sick_leave_california_user_timeoff_assignment_dag_id,
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
            no_task="is_caller_not_add"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="is_caller_not_add",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        is_caller_not_add = rail.IfOperator(
            task_id = "is_caller_not_add",
            test=lambda dag_run: not common_custom_methods.is_caller_add_update_rehire(dag_run, "Add"),
            yes_task="get_tenure",
            no_task="get_default_timeoff_policy_set_schedule_for_timeoff_type"
        )

        get_tenure = rail.PythonOperator(
            task_id = "get_tenure",
            python_callable=lambda dag_run: common_custom_methods.get_tenure_value(
                                                    date_1= common_custom_methods.convert_json_date_to_date(dag_run.conf['start_date']),
                                                    date_2= common_custom_methods.convert_json_date_to_date(dag_run.conf['schedule_changed_date']))
        )

        get_default_timeoff_policy_set_schedule_for_timeoff_type = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy_set_schedule_for_timeoff_type",
            endpoint = "/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda dag_run: {
                "timeOffTypeUri": dag_run.conf['secondary_timeoff_uri'] 
            }
        )

        is_caller_add = rail.IfOperator(
            task_id = "is_caller_add",
            test=lambda dag_run: common_custom_methods.is_caller_add_update_rehire(dag_run, "Add"),
            yes_task="get_policy_to_assign_add",
            no_task="get_policy_to_assign_update"
        )

        def get_policy_to_assign_add_callable(dag_run):
            policies_to_assign = []
            start_date = common_custom_methods.get_date_from_json_date(dag_run.conf['start_date'])
            global_timeoff_policies = rail.result("get_default_timeoff_policy_set_schedule_for_timeoff_type")
            for policy in global_timeoff_policies:
                effective_date = start_date + relativedelta(years=policy['startOffset']['offsetValue'])
                policies_to_assign.append({
                    "description" : f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                    "effectiveDate": {
                        "day": effective_date.day,
                        "month": effective_date.month,
                        "year": effective_date.year
                    },
                    "policySet": policy['policySet']
                })
            return policies_to_assign

        get_policy_to_assign_add = rail.PythonOperator(
            task_id = "get_policy_to_assign_add",
            python_callable=get_policy_to_assign_add_callable
        )

        def get_policy_to_assign_update_callable(dag_run):
            policies_to_assign = []
            # Step 12
            policy_sets = dag_run.conf['policy_sets']
            schedule_changed_date = dag_run.conf['schedule_changed_date']
            start_date = dag_run.conf['start_date']
            policies_to_assign = policies_to_assign + list(filter(lambda policy: common_custom_methods.compare_two_dates(
                                                date1 = common_custom_methods.get_date_from_json_date(policy['effectiveDate']),
                                                date2 = common_custom_methods.get_date_from_json_date(schedule_changed_date),
                                                operator = '<'), policy_sets))
            
            global_timeoff_policies = rail.result("get_default_timeoff_policy_set_schedule_for_timeoff_type")
            tenure = float(rail.result("get_tenure")) if rail.result("get_tenure") else 0
            # Step 18
            policies_sets = []
            current_assigned_policy_found = False
            for policy in global_timeoff_policies:
                if float(policy['startOffset']['offsetValue']) > tenure:
                    policies_sets.append(
                        {
                            "offset": policy['startOffset']['offsetValue'],
                            "policy": policy["policySet"],
                            "first": "No"
                        }
                    )

                if float(policy['startOffset']['offsetValue']) == tenure:
                    policies_sets.append(
                        {
                            "offset": policy['startOffset']['offsetValue'],
                            "policy": policy["policySet"],
                            "first": "Yes"
                        }
                    )
                    current_assigned_policy_found = True
            rail.set_result(key="policies_sets", val=policies_sets)
            if not current_assigned_policy_found:
                policies_from_global = []
                for policy in global_timeoff_policies:
                    if float(policy['startOffset']['offsetValue']) < tenure:
                        policies_from_global.append(
                            {
                                "offset": policy['startOffset']['offsetValue'],
                                "policy": policy["policySet"],
                                "diff": float(policy['startOffset']['offsetValue']) < tenure
                            }
                        )
                if policies_from_global:
                    max_diff = max([i['diff'] for i in policies_from_global])
                    rail.set_result(val=max_diff, key="max_diff")
                    if max_diff:
                        for _policy in policies_from_global:
                            if _policy['diff'] == max_diff:
                                policies_sets.append(
                                    {
                                        "offset": _policy['offset'],
                                        "policy": _policy["policy"],
                                        "first": "Yes"
                                    }
                                )

                for policy_set in policies_sets:
                    if policy_set['first'] == "Yes":
                        policies_to_assign.append(
                            {
                                "description": f"Effective on {schedule_changed_date['day']}/{schedule_changed_date['month']/schedule_changed_date['year']}",
                                "effectiveDate": schedule_changed_date,
                                "policySet": policy_set['policy']
                            }
                        )
                    else:
                        effective_date_to_use = common_custom_methods.get_date_from_json_date(start_date) + relativedelta(years=policy_set['offset'])
                        policies_to_assign.append(
                            {
                                "description": f"Effective on {effective_date_to_use.day}/{effective_date_to_use.month/effective_date_to_use.year}",
                                "effectiveDate": effective_date_to_use,
                                "policySet": policy_set['policy']
                            }
                        )
            return policies_to_assign

        get_policy_to_assign_update = rail.PythonOperator(
            task_id = "get_policy_to_assign_update",
            python_callable=get_policy_to_assign_update_callable
        )

        get_policy_to_assign = rail.PythonOperator(
            task_id = "get_policy_to_assign",
            python_callable=lambda dag_run: rail.result("get_policy_to_assign_add") if common_custom_methods.is_caller_add_update_rehire(
                                                dag_run, 'Add') else rail.result("get_policy_to_assign_update")
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test = lambda: rail.result("get_policy_to_assign"),
            yes_task = "assign_policy_to_user",
            no_task = "catch_and_log_error"
        )

        assign_policy_to_user = rail.RepliconServiceOperator(
            task_id = "assign_policy_to_user",
            endpoint = "/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri']
                },
                "policySetScheduleEntries": loads(dumps(rail.result("get_policy_to_assign")
                                                        ).replace("\"script\"", "\"scriptTarget\""
                                                        ).replace('}},"scriptTarget"', '}}],"scriptTarget"'
                                                        )
                                                    )
            }
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
                "Action": dag_run.conf.get("action", "Update"),
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> is_caller_not_add
        is_caller_not_add >> rail.Label("Yes") >> get_tenure >> get_default_timeoff_policy_set_schedule_for_timeoff_type
        is_caller_not_add >> rail.Label("No") >> get_default_timeoff_policy_set_schedule_for_timeoff_type >> is_caller_add

        is_caller_add >> rail.Label("Yes") >> get_policy_to_assign_add >> get_policy_to_assign
        is_caller_add >> rail.Label("No") >> get_policy_to_assign_update >> get_policy_to_assign

        get_policy_to_assign >> has_any_policy_to_assign >> rail.Label("Yes") >> assign_policy_to_user >> catch_and_log_error
        has_any_policy_to_assign >> rail.Label("No") >> catch_and_log_error

        return dag

rail.for_each_instance(create_us_sick_timeoff_california_dag)
