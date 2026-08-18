from datetime import timedelta
from json import dumps, loads
from dateutil.relativedelta import relativedelta
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_australia_v3.utils.custom_methods \
    import get_weekly_accrual_parameters, is_caller_add_test, has_any_policy_to_assign_test
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods \
    import get_tenure_value
from dxctechnology.workday_user_import_v1.user_import_global_v2.utils import custom_methods as gbl_custom_methods  



null = None

OPEN_BRACKETS = '{'
CLOSE_BRACKETS = '}'


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_australia_users_aus_personal_carers_leave_timeoff_assignment_child_dag,
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
                "timeOffTypeUri": "{{ dag_run.conf.timeoff_type_uri }}"
            }
        )

        is_fte_one = rail.IfOperator(
            task_id = "is_fte_one",
            # 1 can be used, added float(1) for re-assurance
            test = lambda dag_run: float(dag_run.conf['fte']) == float(1),
            yes_task="is_caller_add",
            no_task="is_caller_add2"
        )
        
        is_caller_add = rail.IfOperator(
            task_id = "is_caller_add",
            test= is_caller_add_test,
            yes_task="get_default_timeoff_type_policy_schedule_for_user",
            no_task="empty_caller_is_update"
        )

        get_default_timeoff_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id="get_default_timeoff_type_policy_schedule_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
                }
            }
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test=lambda : bool(rail.result(get_default_timeoff_type_policy_schedule_for_user.task_id) and\
                                rail.result(get_default_timeoff_type_policy_schedule_for_user.task_id)[0]['policySet']),
            yes_task="put_user_timeoff_account_policyset_schedule",
            no_task="stop"
        )

        def get_put_user_timeoff_account_policyset_schedule_payload(dag_run):
            timeoff_policy = loads(dumps(rail.result(get_default_timeoff_type_policy_schedule_for_user.task_id)
                                        ).replace("/null/", "\"effective\""
                                        ).replace("\"script\"", "\"scriptTarget\""
                                        ))
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
                },
                "policySetScheduleEntries": timeoff_policy
            }

        put_user_timeoff_account_policyset_schedule = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_account_policyset_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_user_timeoff_account_policyset_schedule_payload
        )


        empty_caller_is_update = rail.EmptyOperator(
            task_id = "empty_caller_is_update"
        )

        def get_policy_to_assign_callable(dag_run):
            start_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['start_date'])
            schedule_change_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['schedule_change_date'])
            tenure = get_tenure_value(start_date, schedule_change_date)
            
            policy_to_assign = []

            current_timeoff_policies = (loads(dag_run.conf['current_timeoff_policies']) if not isinstance(dag_run.conf['current_timeoff_policies'], list) else dag_run.conf['current_timeoff_policies']) if dag_run.conf['current_timeoff_policies'] else []
            for policy_line in current_timeoff_policies:
                if gbl_custom_methods.convert_json_date_to_date(policy_line['effectiveDate']) < schedule_change_date:
                    policy_to_assign.append(
                        {
                            "description": policy_line['description'],
                            "effectiveDate": policy_line['effectiveDate'],
                            "policySet": policy_line['policySet']
                        }
                    )

            policy_sets = []

            current_policy_assigned_found = False
            default_policy_for_timeoff = rail.result("get_default_policy_for_user")
            for policy in default_policy_for_timeoff:
                if policy['startOffset']['offsetValue'] == tenure:
                    current_policy_assigned_found = True
                    policy_sets.append(
                        {
                            "offset": policy['startOffset']['offsetValue'],
                            "policy": policy['policySet'],
                            "first": "Yes"
                        }
                    )
            
                if policy['startOffset']['offsetValue'] > tenure:
                    policy_sets.append(
                        {
                            "offset": policy['startOffset']['offsetValue'],
                            "policy": policy['policySet'],
                            "first": "No"
                        }
                    )

            if not current_policy_assigned_found:
                off_set_list = list(filter(lambda row: row['to_be_considered']=="Yes", map(lambda _policy: {
                    "offset": _policy['startOffset']['offsetValue'],
                    "day_diff": float(_policy['startOffset']['offsetValue']) - tenure,
                    "to_be_considered" : "Yes" if float(_policy['startOffset']['offsetValue']) < tenure else "No",
                    "policy": _policy['policySet']
                }, default_policy_for_timeoff)))
                max_daydiff = max([offset['day_diff'] for offset in off_set_list]) if off_set_list else null
                if max_daydiff:
                    for item in off_set_list:
                        if item['day_diff'] == tenure:
                            policy_sets.append(
                                {
                                    "offset": item['offset'],
                                    "policy": item['policy'],
                                    "first": "Yes"
                                }
                            )

            for policy_set in policy_sets:
                if policy_set['first'] == "Yes":
                    policy_to_assign.append(
                        {
                            "description": f"Effective on {schedule_change_date.day}/{schedule_change_date.month}/{schedule_change_date.year}",
                            "effectiveDate": {
                                "year": schedule_change_date.year,
                                "month": schedule_change_date.month,
                                "day": schedule_change_date.day
                            },
                            "policySet": policy['policy']
                        }
                    )
                    continue
                effective_date = start_date + relativedelta(years=int(policy_set['offset']))
                policy_to_assign.append(
                    {
                        "description": f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                        "effectiveDate": {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        },
                        "policySet": policy['policy']
                    }
                )

            return loads(dumps(policy_to_assign).replace('''"additionalParameters": [{"keyUri": "urn:replicon:script-key:parameter:amount","value": {"number": 80}},{"keyUri": "urn:replicon:script-key:parameter:precedence","value": {"number": 10}}],''',
                                                        "").replace("\"script\"", "\"scriptTarget\""))

        get_policy_to_assign = rail.PythonOperator(
            task_id = "get_policy_to_assign",
            python_callable=get_policy_to_assign_callable
        )

        has_any_policy_to_assign2 = rail.IfOperator(
            task_id = "has_any_policy_to_assign2",
            test="{{ result('get_policy_to_assign') | is_truthy }}",
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
                "policySetScheduleEntries": rail.result("get_policy_to_assign")
            }
        )

        stop = rail.EmptyOperator(
            task_id = "stop"
        )

        is_caller_add2 = rail.IfOperator(
            task_id = "is_caller_add2",
            test=is_caller_add_test,
            yes_task="get_policy_to_assign_add_fte_not_one",
            no_task="get_policy_to_assign_update_fte_not_one"
        )


        def get_policy_to_assign_add_fte_not_one_callable(dag_run):
            start_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['start_date'])
            fte = float(dag_run.conf['fte'])
            policy_to_assign = []
            policysets = []
            for item in rail.result("get_default_policy_for_user"):
                _, accural_amt_based_on_schedule, get_last_accural_amt = get_weekly_accrual_parameters(
                    item['policySet'].get('timeOffBalanceEventScripts', []),
                    fte)
                # need to check workato behavior when the value is not found
                policysets.append(
                    {
                        "offset": item['startOffset']['offsetValue'],
                        "policy": loads(dumps(item['policySet']).replace(
                                f"""{OPEN_BRACKETS}"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {OPEN_BRACKETS}"number": {get_last_accural_amt}{CLOSE_BRACKETS}{CLOSE_BRACKETS}""",
                                f"""{OPEN_BRACKETS}"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {OPEN_BRACKETS}"number": {accural_amt_based_on_schedule}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                            ).replace(
                                f"""{OPEN_BRACKETS}"keyUri": "urn:replicon:script-key:parameter:amount", "value": {OPEN_BRACKETS}"number": {get_last_accural_amt}{CLOSE_BRACKETS}{CLOSE_BRACKETS}""",
                                f"""{OPEN_BRACKETS}"keyUri": "urn:replicon:script-key:parameter:amount", "value": {OPEN_BRACKETS}"number": {accural_amt_based_on_schedule}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
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

        get_policy_to_assign_add_fte_not_one = rail.PythonOperator(
            task_id = "get_policy_to_assign_add_fte_not_one",
            python_callable=get_policy_to_assign_add_fte_not_one_callable
        )

        def get_policy_to_assign_update_fte_not_one_callable(dag_run):
            start_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['start_date'])
            schedule_change_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['schedule_change_date'])
            fte = dag_run.conf['fte']
            tenure = get_tenure_value(start_date, schedule_change_date)

            current_policy = dag_run.conf['current_timeoff_policies'] if isinstance(dag_run.conf['current_timeoff_policies'], list) else loads(dag_run.conf['current_timeoff_policies'] or [])
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
            current_policy_to_assign_is_found = False
            for policy_line in default_policy:
                if policy_line['startOffset']['offsetValue'] > tenure:
                    _, accrual_amt_based_on_schedule, get_last_accrual_amt = get_weekly_accrual_parameters(
                        policy_line['policySet'].get('timeOffBalanceEventScripts', []),
                        fte)
                    policy_sets.append(
                        {
                            "offset": policy_line['startOffset']['offsetValue'],
                            "policy": loads(dumps(policy_line['policySet']).replace(
                                    f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:amount","value":{OPEN_BRACKETS}"number": {get_last_accrual_amt}{CLOSE_BRACKETS}{CLOSE_BRACKETS}""",
                                    f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:amount","value":{OPEN_BRACKETS}"number": {accrual_amt_based_on_schedule}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                                )),
                            "first": 'No'
                        }
                    )
                if policy_line['startOffset']['offsetValue'] == tenure:
                    _, accrual_amt_based_on_schedule1, get_last_accrual_amt1 = get_weekly_accrual_parameters(
                        policy_line['policySet'].get('timeOffBalanceEventScripts', []),
                        fte)
                    current_policy_to_assign_is_found = True
                    policy_sets.append(
                        {
                            "offset": policy_line['startOffset']['offsetValue'],
                            "policy": loads(dumps(policy_line['policySet']).replace(
                                    f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:amount","value":{OPEN_BRACKETS}"number": {get_last_accrual_amt1}{CLOSE_BRACKETS}{CLOSE_BRACKETS}""",
                                    f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:amount","value":{OPEN_BRACKETS}"number": {accrual_amt_based_on_schedule1}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                                )),
                            "first": 'Yes'
                        }
                    )
            if not current_policy_to_assign_is_found:
                off_set_list = list(filter(lambda row: row['to_be_considered']=="Yes", map(lambda _policy: {
                        "offset": _policy['startOffset']['offsetValue'],
                        "day_diff": float(_policy['startOffset']['offsetValue']) - tenure,
                        "to_be_considered" : "Yes" if float(_policy['startOffset']['offsetValue']) < tenure else "No",
                        "policy": _policy['policySet']
                    }, default_policy)))
                max_daydiff = max([offset['day_diff'] for offset in off_set_list]) if off_set_list else null
                if max_daydiff:
                    for _item in off_set_list:
                        if _item['offset'] == tenure:
                            _, accrual_amt_based_on_schedule3, get_last_accrual_amt3 = get_weekly_accrual_parameters(
                                _item['policy'].get('timeOffBalanceEventScripts', []),
                                fte)
                            policy_sets.append(
                                {
                                    "offset": policy_line['startOffset']['offsetValue'],
                                    "policy": loads(dumps(policy_line['policySet']).replace(
                                            f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:amount","value":{OPEN_BRACKETS}"number": {get_last_accrual_amt3}{CLOSE_BRACKETS}{CLOSE_BRACKETS}""",
                                            f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:amount","value":{OPEN_BRACKETS}"number": {accrual_amt_based_on_schedule3}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                                        )),
                                    "first": 'Yes'
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

        get_policy_to_assign_update_fte_not_one = rail.PythonOperator(
            task_id = "get_policy_to_assign_update_fte_not_one",
            python_callable=get_policy_to_assign_update_fte_not_one_callable
        )

        has_any_policy_to_assign_fte_not_one = rail.IfOperator(
            task_id = "has_any_policy_to_assign_fte_not_one",
            test=lambda dag_run: has_any_policy_to_assign_test(
                dag_run,
                get_policy_to_assign_add_fte_not_one.task_id,
                get_policy_to_assign_update_fte_not_one.task_id
            ),
            yes_task="get_policy_to_assign_fte_not_one",
            no_task="stop"
        )

        get_policy_to_assign_fte_not_one = rail.PythonOperator(
            task_id = "get_policy_to_assign_fte_not_one",
            python_callable=lambda dag_run:  rail.result("get_policy_to_assign_add_fte_not_one") if is_caller_add_test(dag_run) else rail.result("get_policy_to_assign_update_fte_not_one")
        )

        assign_policy_fte_not_one = rail.RepliconServiceOperator(
            task_id = "assign_policy_fte_not_one",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
                },
                "policySetScheduleEntries": rail.result("get_policy_to_assign_fte_not_one")
            }        
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

        get_default_policy_for_user >> is_fte_one >> rail.Label("Yes") >> is_caller_add
        is_caller_add >> rail.Label("Yes") >> get_default_timeoff_type_policy_schedule_for_user >> has_any_policy_to_assign >> rail.Label(
            "Yes")>> put_user_timeoff_account_policyset_schedule >> stop
        has_any_policy_to_assign >> rail.Label("No") >> stop
        is_caller_add >> rail.Label("No") >> empty_caller_is_update >> get_policy_to_assign >> has_any_policy_to_assign2 >> rail.Label(
            "Yes") >> assign_policy >> stop
        has_any_policy_to_assign2 >> rail.Label("No") >> stop

        is_fte_one >> rail.Label("No") >> is_caller_add2 >> rail.Label("Yes") >> get_policy_to_assign_add_fte_not_one >> has_any_policy_to_assign_fte_not_one
        is_caller_add2 >> rail.Label("No") >> get_policy_to_assign_update_fte_not_one >> has_any_policy_to_assign_fte_not_one

        has_any_policy_to_assign_fte_not_one >> rail.Label("Yes") >> get_policy_to_assign_fte_not_one >> assign_policy_fte_not_one >> stop
        has_any_policy_to_assign_fte_not_one >> rail.Label("No") >> stop >> rail.Label("On Error")>> catch_and_log_error


    return dag

rail.for_each_instance(create_dag)













