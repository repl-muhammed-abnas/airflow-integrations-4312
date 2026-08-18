from datetime import timedelta
import json
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import convert_json_date_to_date
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_date_in_json
from decimal import Decimal

def exp_to_decimal_best(exp_str):
    try:
        decimal_num = Decimal(str(exp_str))
        result = format(decimal_num.quantize(Decimal('1.00')), '.2f')
        return result
    except Exception as e:
        return f"Error: {str(e)}"

def create_update_user_timeoff_assignment_for_disabling(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_canada_users_update_user_timeoff_process_child_dag_disable,
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
            yes_task="batch_task",
            no_task="is_end_date_present"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="is_end_date_present",
            end_task="put_user_time_off_account_policy_set_schedule",
            execution_timeout=timedelta(days=14)
        )

        is_end_date_present = rail.IfOperator(
            task_id = "is_end_date_present",
            test="{{ dag_run.conf.end_date_json | is_truthy }}",
            yes_task="get_timeoff_details"
        )

        def get_effective_date_callable(dag_run):
            _effective_date =  convert_json_date_to_date(dag_run.conf['end_date_json'])
            if rail.result("get_timeoff_details")[0]['displayText'].startswith("[AUS]"):
                return _effective_date + timedelta(days=1)
            if dag_run.conf['parent_location']== "Australia":
                return _effective_date + timedelta(days=1)
            return {
                "day": _effective_date.day,
                "month": _effective_date.month,
                "year": _effective_date.year
            }

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id = "get_timeoff_details",
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypes",
            data= {
                "timeOffTypeUris": ["{{dag_run.conf.timeoff_uri}}"]
            }
        )

        get_effective_date = rail.PythonOperator(
            task_id = "get_effective_date",
            python_callable=get_effective_date_callable
        )

        get_balance_summary = rail.RepliconServiceOperator(
            task_id = "get_balance_summary",
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data= lambda dag_run:{
                "account": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri']
                },
                "asOfDate": dag_run.conf['end_date_json']
            }
        )


        def get_put_user_time_off_account_policy_set_schedule_payload(dag_run):
            _data = json.loads(dag_run.conf["policy_set"]) if isinstance(dag_run.conf["policy_set"], str) else dag_run.conf["policy_set"]
            end_date =convert_json_date_to_date(dag_run.conf['end_date_json'])
            policies_to_retain = []
            for policy in _data:
                if convert_json_date_to_date(policy['effectiveDate']) < end_date:
                    policies_to_retain.append(
                        {
                            "effectiveDate": policy['effectiveDate'],
                            "description":policy['description'],
                            "policySet": policy["policySet"]
                        }
                    )
            
            effective_date_to_use = rail.result("get_effective_date")
            policies_to_retain.append(
                {
                    "effectiveDate": effective_date_to_use,
                    "description": f"Added by Integration on {effective_date_to_use['day']}-{effective_date_to_use['month']}-{effective_date_to_use['year']}",
                    "policySet": {
                        "timeOffBalanceEventScripts": [{
                            "additionalParameters": [{
                                "keyUri": "urn:replicon:script-key:parameter:amount",
                                "value": {
                                    "number": exp_to_decimal_best(str(rail.result('get_balance_summary')["timeRemaining"]))
                                }
                            }],
                            "scriptTarget": {
                                "description": "Set initial balance for the first day of a policy",
                                "name": "Starting Balance Set To",
                                "uri": dag_run.conf['starting_balance_set_to_uri']
                            }
                        }],
                        "timeOffValidationScripts":[ {
                            "additionalParameters": [{
                                "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                "value": {
                                    "number": "0"
                                }
                            }],
                            "scriptTarget": {
                                "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                                "name": "Prevent balance overdraw",
                                "uri": dag_run.conf['prevent_balance_overdraw_uri']
                            }
                        }]
                    }
                }
            )
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri']
                },
                "policySetScheduleEntries": json.loads(json.dumps(policies_to_retain).replace("/null/", "\"effective\"")
                                    .replace('\"script\"',"\"scriptTarget\"")
                                    .replace('":{"additionalParameters', '":[{"additionalParameters')
                                    .replace(':{"keyUri"', ':[{"keyUri"').replace('}},"scriptTarget"', '}}],"scriptTarget"')
                                    .replace('}},"timeOffValidationScripts', '}}],"timeOffValidationScripts')
                                    .replace('}}},"description', '}}]},"description'))
            }


        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id = "put_user_time_off_account_policy_set_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_user_time_off_account_policy_set_schedule_payload
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> put_user_time_off_account_policy_set_schedule
        can_run_batch_task >> rail.Label("No") >> is_end_date_present

        is_end_date_present >> rail.Label("Yes") >> get_timeoff_details
        get_timeoff_details >> get_effective_date >> get_balance_summary >> put_user_time_off_account_policy_set_schedule

        return dag

rail.for_each_instance(create_update_user_timeoff_assignment_for_disabling)
