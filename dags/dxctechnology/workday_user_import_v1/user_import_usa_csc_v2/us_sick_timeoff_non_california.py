from datetime import timedelta
from dateutil.relativedelta import relativedelta
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import.common_utils \
    import custom_methods as common_custom_methods

OPEN_BRACKETS = '{'
CLOSE_BRACKETS = '}'

def create_us_sick_timeoff_non_california_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.usa_csc_us_sick_leave_non_california_user_timeoff_assignment_dag_id,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_run_sick_non_cal_to_assignment
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_us_csc, default_var='true').lower() == 'true',
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
                date_2=common_custom_methods.convert_json_date_to_date(dag_run.conf['schedule_changed_date']))
        )

        get_default_timeoff_policy_set_schedule_for_timeoff_type = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy_set_schedule_for_timeoff_type",
            endpoint = "/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda dag_run: {
                "timeOffTypeUri": dag_run.conf['timeoff_uri'] 
            }
        )

        is_fte_one = rail.IfOperator(
            task_id = "is_fte_one",
            test = lambda dag_run: dag_run.conf['fte'] in [1, '1'],
            yes_task= "is_caller_add",
            no_task = "empty_fte_not_one"
        )

        is_caller_add = rail.IfOperator(
            task_id = "is_caller_add",
            test = lambda dag_run: common_custom_methods.is_caller_add_update_rehire(dag_run, "Add"),
            yes_task = "get_default_timeoff_type_policy_schedule_for_user",
            no_task = "get_policy_to_assign_update"
        )

        get_default_timeoff_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_type_policy_schedule_for_user",
            endpoint = "/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data = lambda dag_run : {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri']
                }
            }
        )

        has_any_policy_set_to_assign = rail.IfOperator(
            task_id = "has_any_policy_set_to_assign",
            test = lambda: rail.result("get_default_timeoff_type_policy_schedule_for_user") and rail.result("get_default_timeoff_type_policy_schedule_for_user")[0]['policySet'],
            yes_task = "put_user_timeoff_account_policy_set_schedule",
            no_task="stop"
        )

        put_user_timeoff_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id = "put_user_timeoff_account_policy_set_schedule",
            endpoint = "/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri']
                },
                "policySetScheduleEntries": loads(dumps(rail.result("get_default_timeoff_type_policy_schedule_for_user")).replace("/null/", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                ).replace("null", "\"effective\""
                ))
            }
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
                                        "offset": _policy['startOffset']['offsetValue'],
                                        "policy": _policy["policySet"],
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

        has_policy_to_assign = rail.IfOperator(
            task_id = "has_policy_to_assign",
            test = lambda: bool(rail.result("get_policy_to_assign_update")),
            yes_task = "assign_timeoff_to_user_update",
            no_task = "stop"
        )

        assign_timeoff_to_user_update = rail.RepliconServiceOperator(
            task_id = "assign_timeoff_to_user_update",
            endpoint = "/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                    "timeOffAccount": {
                        "userUri": dag_run.conf['user_uri'],
                        "timeOffTypeUri": dag_run.conf['timeoff_uri']
                    },
                    "policySetScheduleEntries": loads(dumps(rail.result("get_policy_to_assign_update")
                                                        ).replace("\"script\"", "\"scriptTarget\""
                                                        ).replace('}},"scriptTarget"', '}}],"scriptTarget"'
                                                        )
                                                    )
            }
        )

        stop = rail.EmptyOperator(
            task_id = "stop"
        )

        empty_fte_not_one = rail.EmptyOperator(
            task_id = "empty_fte_not_one"
        )


        """
            Step 46 onwards
        """
        is_caller_add_2 = rail.IfOperator(
            task_id = "is_caller_add_2",
            test = lambda dag_run: common_custom_methods.is_caller_add_update_rehire(dag_run, "Add"),
            yes_task = "get_policy_to_assign_add",
            no_task = "get_policy_to_assign_update_fte_not_one"
        )

        def get_policy_to_assign_add_callable(dag_run):
            policy_sets = []
            fte = float(dag_run.conf['fte'])
            for policy in rail.result("get_default_timeoff_policy_set_schedule_for_timeoff_type"):
                weekly_accrual_params = rail.find_first_by_attr_and_get_attr(policy['policySet']['timeOffBalanceEventScripts'],
                                                                            'script.name',
                                                                            'Weekly Accrual',
                                                                            default={})
                weekly_accrual_params = weekly_accrual_params.get('additionalParameters', {})

                # as per workato if there is not value found it returns [] after the pluck('additionalParameters')
                # is executed
                if not weekly_accrual_params:
                    # this will ensure that the behavior is same as the workato
                    weekly_accrual_params = []
                weekly_accrual_params = loads(dumps(weekly_accrual_params).replace("[[{", "[{").replace("}]]","}]"))
                # this should be `null`` however if we convert `null` to `float` in workato its returns `0`
                yearly_accrual_amount = 0
                for item in weekly_accrual_params:
                    if item['keyUri'] == "urn:replicon:script-key:parameter:accrual-annual-amount":
                        yearly_accrual_amount = item['value']['number']
                # Step 52
                yearly_accrual_amount = float(yearly_accrual_amount)
                # Step 53
                accrual_amt_based_on_schedule = yearly_accrual_amount * fte
                value_to_be_gsubbed_from_policy = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:accrual-annual-amount","value":{OPEN_BRACKETS}"number": {weekly_accrual_params}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                actual_value_to_be_added = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:accrual-annual-amount","value":{OPEN_BRACKETS}"number": {accrual_amt_based_on_schedule}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                policy_sets.append(
                    {
                        "offset": policy['startOffset']['offsetValue'],
                        "first": "No",
                        "policy": loads(dumps(policy['policySet']).replace(value_to_be_gsubbed_from_policy, actual_value_to_be_added))
                    }
                )

            policy_to_assign = []
            start_date = common_custom_methods.convert_json_date_to_date(dag_run.conf['start_date'])
            for policy_in_policy_sets in policy_sets:
                effective_date = start_date + relativedelta(years=policy_in_policy_sets['offset'])
                policy_to_assign.append(
                    {
                        "description": f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                        "effectiveDate": {
                            "day": effective_date.day,
                            "month": effective_date.month,
                            "year": effective_date.year
                        },
                        "policySet": policy_in_policy_sets['policy']
                    }
                )
            return policy_to_assign

        get_policy_to_assign_add = rail.PythonOperator(
            task_id = "get_policy_to_assign_add",
            python_callable=get_policy_to_assign_add_callable
        )

        def get_policy_to_assign_update_fte_not_one_callable(dag_run):
            policy_sets_from_master = dag_run.conf['policy_sets']
            fte = float(dag_run.conf['fte'])
            policy_sets = []
            schedule_changed_date = dag_run.conf['schedule_changed_date']
            start_date = common_custom_methods.convert_json_date_to_date(dag_run.conf['start_date'])
            policies_to_assign = []

            policies_to_assign = policies_to_assign + list(filter(lambda policy_line: common_custom_methods.compare_two_dates(
                                                date1 = common_custom_methods.get_date_from_json_date(policy_line['effectiveDate']),
                                                operator = '<',
                                                date2 = common_custom_methods.get_date_from_json_date(schedule_changed_date)
                                                ), policy_sets_from_master))
            
            global_timeoff_policies = rail.result("get_default_timeoff_policy_set_schedule_for_timeoff_type")
            tenure = float(rail.result("get_tenure")) if rail.result("get_tenure") else 0
            current_policy_to_assign_is_found = None
            for _policy in global_timeoff_policies:
                if float(_policy['startOffset']['offsetValue']) > tenure:
                    weekly_accrual_params = rail.find_first_by_attr_and_get_attr(_policy['policySet']['timeOffBalanceEventScripts'],
                                                                            'script.name',
                                                                            'Weekly Accrual'
                                                                            'additionalParameters')
                    # as per workato if there is not value found it returns [] after the pluck('additionalParameters')
                    # is executed
                    if not weekly_accrual_params:
                        # this will ensure that the behavior is same as the workato
                        weekly_accrual_params = []
                    weekly_accrual_params = loads(dumps(weekly_accrual_params).replace("[[{", "[{").replace("}]]","}]"))
                    # this should be `null`` however if we convert `null` to `float` in workato its returns `0`
                    yearly_accrual_amount = 0
                    for item in weekly_accrual_params:
                        if item['keyUri'] == "urn:replicon:script-key:parameter:accrual-annual-amount":
                            yearly_accrual_amount = item['value']['number']
                    # Step 52
                    yearly_accrual_amount = float(yearly_accrual_amount)
                    accrual_amt_based_on_schedule = yearly_accrual_amount * fte
                    value_to_be_gsubbed_from_policy = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:accrual-annual-amount","value":{OPEN_BRACKETS}"number": {weekly_accrual_params}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                    actual_value_to_be_added = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:accrual-annual-amount","value":{OPEN_BRACKETS}"number": {accrual_amt_based_on_schedule}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                    policy_sets.append(
                        {
                            "offset": _policy['startOffset']['offsetValue'],
                            "first": "No",
                            "policy": loads(dumps(_policy['policySet']).replace(value_to_be_gsubbed_from_policy, actual_value_to_be_added))
                        }
                    )
                # step 81
                if float(_policy['startOffset']['offsetValue']) == tenure:
                    weekly_accrual_params = rail.find_first_by_attr_and_get_attr(_policy['policySet']['timeOffBalanceEventScripts'],
                                                                            'script.name',
                                                                            'Weekly Accrual'
                                                                            'additionalParameters')
                    # as per workato if there is not value found it returns [] after the pluck('additionalParameters')
                    # is executed
                    if not weekly_accrual_params:
                        # this will ensure that the behavior is same as the workato
                        weekly_accrual_params = []
                    weekly_accrual_params = loads(dumps(weekly_accrual_params).replace("[[{", "[{").replace("}]]","}]"))
                    # this should be `null`` however if we convert `null` to `float` in workato its returns `0`
                    yearly_accrual_amount = 0
                    for item in weekly_accrual_params:
                        if item['keyUri'] == "urn:replicon:script-key:parameter:accrual-annual-amount":
                            yearly_accrual_amount = item['value']['number']
                    # Step 52
                    yearly_accrual_amount = float(yearly_accrual_amount)
                    accrual_amt_based_on_schedule = yearly_accrual_amount * fte
                    value_to_be_gsubbed_from_policy =  f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:accrual-annual-amount","value":{OPEN_BRACKETS}"number": {weekly_accrual_params}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                    actual_value_to_be_added =  f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:accrual-annual-amount","value":{OPEN_BRACKETS}"number": {accrual_amt_based_on_schedule}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                    policy_sets.append(
                        {
                            "offset": _policy['startOffset']['offsetValue'],
                            "first": "No",
                            "policy": loads(dumps(_policy['policySet']).replace(value_to_be_gsubbed_from_policy, actual_value_to_be_added))
                        }
                    )
                current_policy_to_assign_is_found = True
            if not current_policy_to_assign_is_found:
                policies_from_global = list(map(lambda item: {
                    "offset": item['startOffset']['offsetValue'],
                    "policy": item['policySet'],
                    "diff": float(item['startOffset']['offsetValue']) - tenure
                },filter(lambda gbl_policy_line: float(
                    gbl_policy_line['startOffset']['offsetValue']) < tenure, global_timeoff_policies)))
                max_diff = None
                if policies_from_global:
                    max_diff = max([i['diff'] for i in policies_from_global])
                if max_diff:
                    for glb_policy in policies_from_global:
                        if max_diff == glb_policy['diff']:
                            _weekly_accrual_params = rail.find_first_by_attr_and_get_attr(_policy['policySet']['timeOffBalanceEventScripts'],
                                                                            'script.name',
                                                                            'Weekly Accrual'
                                                                            'additionalParameters')
                            # as per workato if there is not value found it returns [] after the pluck('additionalParameters')
                            # is executed
                            if not _weekly_accrual_params:
                                # this will ensure that the behavior is same as the workato
                                _weekly_accrual_params = []
                            _weekly_accrual_params = loads(dumps(_weekly_accrual_params).replace("[[{", "[{").replace("}]]","}]"))
                            # this should be `null`` however if we convert `null` to `float` in workato its returns `0`
                            _yearly_accrual_amount = 0
                            for item in _weekly_accrual_params:
                                if item['keyUri'] == "urn:replicon:script-key:parameter:accrual-annual-amount":
                                    _yearly_accrual_amount = item['value']['number']
                            # Step 52
                            _yearly_accrual_amount = float(_yearly_accrual_amount)
                            _accrual_amt_based_on_schedule = _yearly_accrual_amount * fte
                            _value_to_be_gsubbed_from_policy = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:accrual-annual-amount","value":{OPEN_BRACKETS}"number": {_weekly_accrual_params}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                            _actual_value_to_be_added = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:accrual-annual-amount","value":{OPEN_BRACKETS}"number": {_accrual_amt_based_on_schedule}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
                            policy_sets.append(
                                {
                                    "offset": _policy['startOffset']['offsetValue'],
                                    "first": "Yes",
                                    "policy": loads(dumps(glb_policy['policySet']).replace(_value_to_be_gsubbed_from_policy, _actual_value_to_be_added))
                                }
                            )

            effective_date_for_first_policy = common_custom_methods.get_date_from_json_date(schedule_changed_date)

            for _policy_to_assign in policy_sets:
                if _policy_to_assign['first'] == 'Yes':
                    policies_to_assign.append(
                        {
                            "description": f"Effective on {effective_date_for_first_policy.day}/{effective_date_for_first_policy.month}/{effective_date_for_first_policy.year}",
                            "effectiveDate": {
                                "day": effective_date_for_first_policy.day,
                                "month": effective_date_for_first_policy.month,
                                "year": effective_date_for_first_policy.year
                            },
                            "policySet": _policy_to_assign['policy']
                        }
                    )
                else:
                    effective_date = start_date + relativedelta(years=_policy_to_assign['offset'])
                    policies_to_assign.append(
                        {
                            "description": f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                            "effectiveDate": {
                                "day": effective_date.day,
                                "month": effective_date.month,
                                "year": effective_date.year
                            },
                            "policySet": _policy_to_assign['policy']
                        }
                    )


            return policies_to_assign

        get_policy_to_assign_update_fte_not_one = rail.PythonOperator(
            task_id = "get_policy_to_assign_update_fte_not_one",
            python_callable=get_policy_to_assign_update_fte_not_one_callable
        )

        def is_policy_to_assign_present_test(dag_run):
            if common_custom_methods.is_caller_add_update_rehire(dag_run, "Add"):
                return bool(rail.result('get_policy_to_assign_add'))
            return bool(rail.result('get_policy_to_assign_update_fte_not_one'))


        is_policy_to_assign_present = rail.IfOperator(
            task_id = "is_policy_to_assign_present",
            test = is_policy_to_assign_present_test,
            yes_task = "put_user_time_off_account_policy_set_schedule",
            no_task = "catch_and_log_error"
        )

        def put_user_time_off_account_policy_set_schedule_payload(dag_run):
            policy_set_schedule = rail.result('get_policy_to_assign_add') if common_custom_methods.is_caller_add_update_rehire(
                dag_run, "Add") else rail.result('get_policy_to_assign_update_fte_not_one')
            policy_set_schedule = loads(dumps(policy_set_schedule
                                                ).replace("\"script\"", "\"scriptTarget\""
                                                ).replace('}},"scriptTarget"', '}}],"scriptTarget"')
                                        )
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri']
                },
                "policySetScheduleEntries": policy_set_schedule
              }


        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id = "put_user_time_off_account_policy_set_schedule",
            endpoint = "services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data= put_user_time_off_account_policy_set_schedule_payload
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
                "Action": 'Add',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> is_caller_not_add

        is_caller_not_add >> rail.Label("Yes") >> get_tenure >> get_default_timeoff_policy_set_schedule_for_timeoff_type
        is_caller_not_add >> rail.Label("No") >> get_default_timeoff_policy_set_schedule_for_timeoff_type

        get_default_timeoff_policy_set_schedule_for_timeoff_type >> is_fte_one

        is_fte_one >> rail.Label("Yes") >> is_caller_add >> rail.Label("Yes") >> get_default_timeoff_type_policy_schedule_for_user
        get_default_timeoff_type_policy_schedule_for_user >> has_any_policy_set_to_assign >> rail.Label(
            "Yes") >> put_user_timeoff_account_policy_set_schedule >> stop
        has_any_policy_set_to_assign >> stop

        is_caller_add >> rail.Label("No") >> get_policy_to_assign_update >> has_policy_to_assign >> rail.Label("No") >> stop >> catch_and_log_error
        has_policy_to_assign >> rail.Label("Yes") >> assign_timeoff_to_user_update >> stop >> catch_and_log_error

        is_fte_one >> rail.Label("No") >> empty_fte_not_one
        empty_fte_not_one >> is_caller_add_2 >> rail.Label("Yes") >> get_policy_to_assign_add >> is_policy_to_assign_present
        is_caller_add_2 >> rail.Label("No") >> get_policy_to_assign_update_fte_not_one >> is_policy_to_assign_present
        is_policy_to_assign_present >> rail.Label("Yes") >> put_user_time_off_account_policy_set_schedule >> catch_and_log_error
        is_policy_to_assign_present >> rail.Label("No") >> catch_and_log_error

    return dag

rail.for_each_instance(create_us_sick_timeoff_non_california_dag)
