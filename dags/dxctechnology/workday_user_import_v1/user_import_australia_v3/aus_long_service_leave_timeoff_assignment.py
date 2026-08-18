from datetime import timedelta
from json import dumps, loads
from dateutil.relativedelta import relativedelta
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_global_v2.utils import custom_methods as gbl_custom_methods  
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods \
    import get_tenure_value, OPEN_BRACKETS, CLOSE_BRACKETS
from dxctechnology.workday_user_import_v1.user_import_australia_v3.utils.response_filter import get_transactions_history_aus_prorata_accrual_timeoff_data_handler

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_australia_users_aus_long_service_leave_timeoff_assignment_child_dag,
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
        
        get_user_timeoff_balance_summary = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_balance_summary",
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['Secondarytimeoffuri']
                },
                "asOfDate": dag_run.conf['json_formatted_dates']['schedule_change_date']
            }
        )

        get_transactions_history_aus_prorata_accural_timeoff = rail.RepliconServiceOperator(
            task_id = "get_transactions_history_aus_prorata_accural_timeoff",
            endpoint = "/services/TimeOffService2.svc/GetTransactionsHistoryForAccount",
            data=lambda dag_run: {
                    "account": {
                        "userUri": dag_run.conf['user_uri'],
                        "timeOffTypeUri": dag_run.conf['aus_prorata_accrual_uri']
                    },
                    "dateRange": {
                        "endDate": dag_run.conf['json_formatted_dates']['location_change_effective_day'],
                        "startDate": dag_run.conf['json_formatted_dates']['2_months_before_location_effective_date'],
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    }
                },
            data_handler=get_transactions_history_aus_prorata_accrual_timeoff_data_handler
        )

        
        def _add_new_policy_line(dag_run, effective_date, balance_for_update):
            return {
                "effectiveDate": effective_date,
                "description": f"Added by Integration on {effective_date['day']}/{effective_date['month']}/{effective_date['year']}",
                "policySet": {
                    "timeOffBalanceEventScripts": [{
                        "additionalParameters": [{
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "number": balance_for_update
                            }
                        }],
                        "script": {
                            "description": "Set initial balance for the first day of a policy",
                            "name": "Starting Balance Set To",
                            "uri": dag_run.conf['starting_balance_set_to_uri']
                        }
                    }],
                    "timeOffValidationScripts": [{
                        "additionalParameters": [{
                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                            "value": {
                                "number": "0"
                            }
                        }],
                        "script": {
                            "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                            "name": "Prevent balance overdraw",
                            "uri": dag_run.conf['prevent_balance_overdraw_uri']
                        }
                    }]
                }
            }
        
        def get_updated_policy_line(_policy, policy_to_pluck_from_default):
            current_starting_balance_set_to_additional_parameters = rail.find_first_by_attr_and_get_attr(
                    _policy["timeOffBalanceEventScripts"],
                    "script.name",
                    "Starting Balance Set To",
                    "additionalParameters"
                )
            
            # not adding default as to have the next step fail 
            # Starting balance is one of the mandatory policy for the AUS LSL state level timeoff's
            current_starting_balance_set_to_value = rail.find_first_by_attr_and_get_attr(
                current_starting_balance_set_to_additional_parameters,
                "keyUri",
                "urn:replicon:script-key:parameter:amount",
                "value"
            )
            
            _transactions_history_aus_prorata_accural_timeoff = rail.result(get_transactions_history_aus_prorata_accural_timeoff.task_id)

            _starting_balance_current_value = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:amount","value":{OPEN_BRACKETS}"number":{current_starting_balance_set_to_value['number']}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
            _starting_balance_value_to_be_updated = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:amount","value":{OPEN_BRACKETS}"number":{_transactions_history_aus_prorata_accural_timeoff['starting_balance_to_update_value']}{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""

            current_prorata_balance_set_to_additional_parameters = rail.find_first_by_attr_and_get_attr(
                    _policy["timeOffBalanceEventScripts"],
                    "script.name",
                    policy_to_pluck_from_default,
                    "additionalParameters"
                )
            
            # not adding default as to have the next step fail 
            # LSL Accrual Redundancy Yearly is one of the mandatory policy for the AUS LSL state level timeoff's
            current_prorata_balance_set_to_value = rail.find_first_by_attr_and_get_attr(
                current_prorata_balance_set_to_additional_parameters,
                "keyUri",
                "urn:replicon:script-key:parameter:prorated-balance",
                "value"
            )

            _prorated_balance_current_value = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:prorated-balance","value":{OPEN_BRACKETS}"text":"{current_prorata_balance_set_to_value}"{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""
            _prorated_balance_value_to_be_updated = f"""{OPEN_BRACKETS}"keyUri":"urn:replicon:script-key:parameter:prorated-balance","value":{OPEN_BRACKETS}"text":"{_transactions_history_aus_prorata_accural_timeoff['prorata_balance_to_be_added_in_policy']}"{CLOSE_BRACKETS}{CLOSE_BRACKETS}"""


            return loads(dumps(_policy).replace(
                    _starting_balance_current_value, _starting_balance_value_to_be_updated
                ).replace(
                    _prorated_balance_current_value,
                    _prorated_balance_value_to_be_updated
                ))

        def get_policy_to_assign_callable(dag_run, add_new_policy_line=True):
            start_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['location_effective_date'])
            schedule_change_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['schedule_change_date'])
            tenure = get_tenure_value(start_date, schedule_change_date)
            
            # Determine if starting balance policy should be added based on state and tenure
            timeoff_name = dag_run.conf['timeoff_type_name'].lower()

            can_add_starting_balance_policy = False
            if ('victoria' in timeoff_name and tenure > 6.99) or ('queensland' in timeoff_name and tenure > 14.99):
                can_add_starting_balance_policy = True

            policy_to_check = "LSL Accrual Redundancy Yearly"
            if can_add_starting_balance_policy:
                policy_to_check = "LSL Accrual Redundancy Daily"

            default_policy_for_timeoff = rail.result("get_default_policy_for_user")
            policy_sets = []
            policy_to_assign = []
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

            for _policy_set in policy_sets:
                if _policy_set['first'] == "Yes":                    
                    policy_to_assign.append(
                        {
                            "description": f"Effective on {schedule_change_date.day}/{schedule_change_date.month}/{schedule_change_date.year}",
                            "effectiveDate": {
                                "year": schedule_change_date.year,
                                "month": schedule_change_date.month,
                                "day": schedule_change_date.day
                            },
                            "policySet": get_updated_policy_line(_policy_set['policy'], policy_to_check)
                        }
                    )
                    continue
                effective_date = start_date + relativedelta(years=int(_policy_set['offset']))
                policy_to_assign.append(
                    {
                        "description": f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                        "effectiveDate": {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        },
                        "policySet": _policy_set['policy']
                    }
                )

            if can_add_starting_balance_policy:
                policy_to_assign(
                    _add_new_policy_line(
                        dag_run,
                        effective_date = dag_run.conf['json_formatted_dates']['schedule_change_date'],
                        balance_for_update = rail.result(
                                                    get_transactions_history_aus_prorata_accural_timeoff.task_id
                                                )['starting_balance_to_update_value']
                    )
                )
            
            return loads(dumps(policy_to_assign).replace("\"script\"", "\"scriptTarget\""))

        get_policy_to_assign = rail.PythonOperator(
            task_id = "get_policy_to_assign",
            python_callable=get_policy_to_assign_callable
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test=lambda: bool(rail.result("get_policy_to_assign")),
            yes_task="assign_policy",
            no_task="stop"
        )

        stop = rail.EmptyOperator(
            task_id="stop"
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

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> rail.Label("On error") >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_default_policy_for_user

        get_default_policy_for_user >> get_user_timeoff_balance_summary >> get_transactions_history_aus_prorata_accural_timeoff >> get_policy_to_assign
        get_policy_to_assign >> has_any_policy_to_assign >> rail.Label("Yes") >> assign_policy >> stop
        has_any_policy_to_assign >> rail.Label("No") >> stop >> rail.Label("On error") >> catch_and_log_error

    return dag

rail.for_each_instance(create_dag)