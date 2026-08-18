
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.timeoff_policy_update_for_no_accrual_child_dag_id,
        description=f'VelawG3_Child for timeoff policy update on each time off type for no accrual V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='build_policy_sets'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='build_policy_sets',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        build_policy_sets = rail.PythonOperator(
            task_id='build_policy_sets',
            python_callable=lambda dag_run: list(filter(lambda x: x['consider'] == "Yes", map(lambda item: {
                "description": item['description'],
                "effectiveDate": {
                    "day": item['effectiveDate']['day'],
                    "month": item['effectiveDate']['month'],
                    "year": item['effectiveDate']['year']
                },
                "policySet": item['policySet'],
                "consider": "Yes" if datetime.strptime((str(item['effectiveDate']['year']) + "/" + str(item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['day'])), "%Y/%m/%d") < datetime.strptime(dag_run.conf['enddateyear'] + "/" + dag_run.conf['enddatemonth'] + "/" + dag_run.conf['enddateday'], "%Y/%m/%d") else "No",
            }, dag_run.conf['policyset'])))

        )

        declare_list_9 = rail.SetVariableOperator(
            task_id='declare_list_9',
            append=False,
            name='new timeoff policy line',
            value=[]
        )

        insert_to_list_10 = rail.SetVariableOperator(
            task_id='insert_to_list_10',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value=lambda dag_run: {
                "policySet": {
                    "timeOffBalanceEventScripts": {
                        "additionalParameters": {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "number": dag_run.conf['newschedulebalance']
                            }
                        },
                        "script": {
                            "description": "Set initial balance for the first day of a policy",
                            "name": "Starting Balance Set To",
                            "uri": dag_run.conf['startingbalancesettouri']
                        }
                    },
                    "timeOffValidationScripts": {
                        "additionalParameters": {
                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                            "value": {
                                "number": "0"
                            }
                        },
                        "script": {
                            "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                            "name": "Prevent balance overdraw",
                            "uri": dag_run.conf['preventbalanceoverdrawuri']
                        }
                    }
                }
            }
        )

        insert_to_list_11 = rail.SetVariableOperator(
            task_id='insert_to_list_11',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value=lambda dag_run: {
                "policySet": rail.result('insert_to_list_10')['value'][0]['policySet'],
                "description": "Added by Integration on " + dag_run.conf['enddateday'] + "-" + dag_run.conf['enddatemonth'] + "-" + dag_run.conf['enddateyear'],
                "effectiveDate": {
                    "day": dag_run.conf['enddateday'],
                    "month": dag_run.conf['enddatemonth'],
                    "year": dag_run.conf['enddateyear']
                }
            }
        )

        log_timeoff_policytoassign_12 = rail.PythonOperator(
            task_id='log_timeoff_policytoassign_12',
            python_callable=lambda:  json.loads(json.dumps(rail.result('build_policy_sets'), ensure_ascii=False).replace("null", '"effective"')
                                                .replace('"script"', '"scriptTarget"')
                                                .replace('":{"additionalParameters', '":[{"additionalParameters')
                                                .replace(':{"keyUri"', ':[{"keyUri"').replace('}},"scriptTarget"', '}}],"scriptTarget"')
                                                .replace('}},"timeOffValidationScripts', '}}],"timeOffValidationScripts')
                                                .replace('}}},"description', '}}]},"description'))
        )

        put_user_time_off_account_policy_set_schedule_13 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_13',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policytoassign_12')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> build_policy_sets >> declare_list_9 \
            >> insert_to_list_10 >> insert_to_list_11 >> log_timeoff_policytoassign_12 \
            >> put_user_time_off_account_policy_set_schedule_13 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
