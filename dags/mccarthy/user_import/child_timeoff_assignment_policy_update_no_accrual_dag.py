from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


def create_termination_policy_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_timeoff_assignment_policy_update_no_accrual_child_{config.instance}',
        description=f'User Sync_Timeoff Assignment Policy Update - No Accrual {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_timeoff_policies_to_assign'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_timeoff_policies_to_assign',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_timeoff_policies_assign():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            policy_schedule = dag_run_conf['policyset']
            enddate_datetime = datetime.strptime(
                dag_run_conf['enddate'], '%m/%d/%Y')
            timeoff_policies = []
            if policy_schedule:
                for item in policy_schedule:
                    effective_date = f"{item['effectiveDate']['month']}/{item['effectiveDate']['day']}/{item['effectiveDate']['year']}"
                    effective_datetime = datetime.strptime(
                        effective_date, '%m/%d/%Y')
                    if effective_datetime < enddate_datetime:
                        timeoff_policies.append({
                            'effectiveDate': item['effectiveDate'],
                            'description': item['description'],
                            'policySet': item['policySet']
                        })
            timeoff_policies.append({
                "effectiveDate": {
                    'year': enddate_datetime.year,
                    'month': enddate_datetime.month,
                    'day': enddate_datetime.day
                },
                "description": f"Added by Integration on {enddate_datetime.day}-{enddate_datetime.month}-{enddate_datetime.year}",
                "policySet": {
                    "timeOffBalanceEventScripts": [
                        {
                            "additionalParameters": [
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:amount",
                                    "value": {
                                        "number": 0
                                    }
                                }
                            ],
                            "scriptTarget": {
                                "description": "Set initial balance for the first day of a policy",
                                "name": "Starting Balance Set To",
                                "uri": dag_run_conf['startingbalancesettouri']
                            }
                        }
                    ],
                    "timeOffValidationScripts": [{
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                "value": {
                                    "number": 0
                                }
                            }
                        ],
                        "scriptTarget": {
                            "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                            "name": "Prevent balance overdraw",
                            "uri": dag_run_conf['preventbalanceoverdrawuri']
                        }
                    }]
                }
            })
            return json.loads(json.dumps(
                timeoff_policies, ensure_ascii=False).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        get_timeoff_policies_to_assign = rail.PythonOperator(
            task_id='get_timeoff_policies_to_assign',
            python_callable=get_timeoff_policies_assign
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_timeoff_policies_to_assign')
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_timeoff_policies_to_assign >> put_user_time_off_account_policy_set_schedule >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_termination_policy_dag)
