from json import dumps, loads
from dateutil.relativedelta import relativedelta
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_global.utils import custom_methods as gbl_custom_methods  
from decimal import Decimal
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods \
    import get_tenure_value
from datetime import timedelta


null = None

def exp_to_decimal_best(exp_str):
    try:
        decimal_num = Decimal(str(exp_str))
        result = format(decimal_num.quantize(Decimal('1.00')), '.2f')
        return result
    except Exception as e:
        return f"Error: {str(e)}"

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_australia_users_aus_annual_leave_timeoff_assignment_child_dag,
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
            no_task="get_default_policy_for_timeoff"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_default_policy_for_timeoff",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        get_default_policy_for_timeoff = rail.RepliconServiceOperator(
            task_id = "get_default_policy_for_timeoff",
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

        def get_new_policy_line_to_add(dag_run, _date):
            return {
                "effectiveDate": {
                    "day": _date.day,
                    "month": _date.month,
                    "year": _date.year
                },
                "description": f"Added by Integration on {_date.strftime('%d-%m-%Y')}",
                "policySet": {
                    "timeOffBalanceEventScripts": [{
                        "additionalParameters": [{
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "number": exp_to_decimal_best(str(rail.result('get_user_timeoff_balance_summary')["timeRemaining"]))
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

        def get_policy_to_assign_callable(dag_run, add_new_policy_line=True):
            start_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['start_date'])
            schedule_change_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['schedule_change_date'])
            tenure = get_tenure_value(start_date, schedule_change_date)
            current_policy_assigned_found = False

            default_policy_for_timeoff = rail.result("get_default_policy_for_timeoff")
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
                            "policySet": policy_set['policy']
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
                        "policySet": policy_set['policy']
                    }
                )
            if add_new_policy_line:
                policy_to_assign.append(get_new_policy_line_to_add(dag_run, schedule_change_date))
            
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
        can_run_batch_task >> rail.Label("No") >> get_default_policy_for_timeoff

        get_default_policy_for_timeoff >> get_user_timeoff_balance_summary >> get_policy_to_assign
        get_policy_to_assign >> has_any_policy_to_assign >> rail.Label("Yes") >> assign_policy >> stop
        has_any_policy_to_assign >> rail.Label("No") >> stop >> rail.Label("On error") >> catch_and_log_error

    return dag

rail.for_each_instance(create_dag)