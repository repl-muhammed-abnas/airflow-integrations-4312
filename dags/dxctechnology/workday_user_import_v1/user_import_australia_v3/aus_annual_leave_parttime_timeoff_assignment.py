from json import dumps, loads
from dateutil.relativedelta import relativedelta
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_global_v2.utils import custom_methods as gbl_custom_methods  
from dxctechnology.workday_user_import_v1.user_import_australia_v3.utils.custom_methods \
    import get_prevent_balance_overdraw, is_caller_add_test, has_any_policy_to_assign_test
from datetime import timedelta


null = None

OPEN_BRACKETS = '{'
CLOSE_BRACKETS = '}'

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_australia_users_aus_annual_leave_parttime_timeoff_assignment_child_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.timeoff_process_max_active_run
    ) as dag:

        
        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_australia, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_default_policy_for_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_default_policy_for_user",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        get_default_policy_for_user = rail.RepliconServiceOperator(
            task_id = "get_default_policy_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.Secondarytimeoffuri }}"
            }
        )

        is_caller_add = rail.IfOperator(
            task_id = "is_caller_add",
            test=is_caller_add_test,
            yes_task="get_policy_to_assign_add",
            no_task="get_policy_to_assign_update"
        )

        def get_policy_to_assign_add_callable(dag_run):
            start_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['start_date'])
            fte = dag_run.conf['fte']
            policy_to_assign = []
            policysets = []

            for item in rail.result("get_default_policy_for_user"):
                _, accural_amt_based_on_schedule, get_last_accural_amt = get_prevent_balance_overdraw(
                    item['policySet'].get('timeOffValidationScripts', []),
                    fte)
                # need to check workato behavior when the value is not found
                policysets.append(
                    {
                        "offset": item['startOffset']['offsetValue'],
                        "policy": loads(dumps(item['policySet']).replace(
                                f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:maximum-overdraw","value":{OPEN_BRACKETS}"number":{get_last_accural_amt}{CLOSE_BRACKETS}{CLOSE_BRACKETS}""",
                                f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:maximum-overdraw","value":{OPEN_BRACKETS}"number":{accural_amt_based_on_schedule}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                            )),
                        "first": 'No'
                    }
                )
            for policy in policysets:
                effective_date = start_date + relativedelta(years=policy['offset'])
                policy_to_assign.append(
                    {
                        'description' : f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                        "effectiveDate" : {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        },
                        "policySet": policy['policy']

                    }
                )

            
            return loads(dumps(policy_to_assign).replace("\"script\"", "\"scriptTarget\""))

        def get_policy_to_assign_update_callable(dag_run):
            start_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['start_date'])
            schedule_change_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['schedule_change_date'])
            fte = dag_run.conf['fte']

            current_policy = loads(dag_run.conf['current_timeoff_policies']) if dag_run.conf['current_timeoff_policies'] else []
            default_policy = rail.result("get_default_policy_for_user")

            policy_to_assign = []
            for policy in current_policy:
                if gbl_custom_methods.convert_json_date_to_date(policy['effectiveDate']) < schedule_change_date:
                    policy_to_assign.append(
                        {
                            "description" : policy['description'],
                            "effectiveDate" : policy['effectiveDate'],
                            "policySet": policy['policy']
                        }
                    )

            policy_sets = []
            
            for idx, policy_line in enumerate(default_policy):
                _, accural_amt_based_on_schedule, get_last_accural_amt = get_prevent_balance_overdraw(
                    policy_line['policySet'].get('timeOffValidationScripts', []),
                    fte)
                policy_sets.append(
                    {
                        "offset": policy_line['startOffset']['offsetValue'],
                        "policy": loads(dumps(policy_line['policySet']).replace(
                                f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:maximum-overdraw","value":{OPEN_BRACKETS}"number":{get_last_accural_amt}{CLOSE_BRACKETS}{CLOSE_BRACKETS}""",
                                f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:maximum-overdraw","value":{OPEN_BRACKETS}"number":{accural_amt_based_on_schedule}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                            )),
                        "first": 'Yes' if idx==0 else 'No'
                    }
                )
            for each_policy in policy_sets:
                if each_policy['first'] == "Yes":
                    policy_to_assign.append(
                            {
                                'description' : f"Effective on {schedule_change_date.day}/{schedule_change_date.month}/{schedule_change_date.year}",
                                "effectiveDate" : {
                                    "year": schedule_change_date.year,
                                    "month": schedule_change_date.month,
                                    "day": schedule_change_date.day
                                },
                                "policySet": each_policy['policy']

                            }

                    )
                    continue
                effective_date = start_date + relativedelta(years=int(each_policy['offset']))
                policy_to_assign.append(
                    {
                        'description' : f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                        "effectiveDate" : {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        },
                        "policySet": each_policy['policy']

                    }
                )

            return loads(dumps(policy_to_assign).replace("\"script\"", "\"scriptTarget\""))

        get_policy_to_assign_add = rail.PythonOperator(
            task_id = "get_policy_to_assign_add",
            python_callable=get_policy_to_assign_add_callable
        )

        get_policy_to_assign_update = rail.PythonOperator(
            task_id = "get_policy_to_assign_update",
            python_callable=get_policy_to_assign_update_callable
        )

        has_any_policy_to_assign_fte_not_one = rail.IfOperator(
            task_id = "has_any_policy_to_assign_fte_not_one",
            test=lambda dag_run: has_any_policy_to_assign_test(
                dag_run,
                get_policy_to_assign_add.task_id,
                get_policy_to_assign_update.task_id
            ),
            yes_task="assign_policy",
            no_task="stop"
        )

        assign_policy = rail.RepliconServiceOperator(
            task_id = "assign_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
                },
                "policySetScheduleEntries": rail.result("has_any_policy_to_assign_fte_not_one", 'policy_to_use')
            }        
        )

        stop = rail.EmptyOperator(
            task_id = "stop"
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{dag_run.conf.user_log}}",
            message = "User Update Error",
            severity = "Error",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['emp_id'],
                "Email": dag_run.conf['email_id'],
                "Action": "Update",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_default_policy_for_user

        get_default_policy_for_user >> is_caller_add >> rail.Label("Yes") >> get_policy_to_assign_add >> has_any_policy_to_assign_fte_not_one
        is_caller_add >> rail.Label("No") >> get_policy_to_assign_update >> has_any_policy_to_assign_fte_not_one

        has_any_policy_to_assign_fte_not_one >> rail.Label("Yes") >> assign_policy >> stop
        has_any_policy_to_assign_fte_not_one >> rail.Label("No") >> stop >> rail.Label("On Error") >> catch_and_log_error

        return dag
    
rail.for_each_instance(create_dag)
