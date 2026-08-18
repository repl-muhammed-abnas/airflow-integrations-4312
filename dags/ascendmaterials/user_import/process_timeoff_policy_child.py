from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.timeoff_policy_dag_id,
        description=f'Ascend_Child for timeoff policy update on each time off type for no accrual {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_scripts_time_off_validation_script'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_scripts_time_off_validation_script',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_scripts_time_off_validation_script = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_validation_script',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Prevent balance overdraw', 'uri', '')
        )

        get_all_scripts_time_off_balance_event_script = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_balance_event_script',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        # Build the complete policy set schedule in one step:
        # 1. Filter existing entries where daydiff > 0 (enddate is after effective date)
        # 2. Append a new "Updated by Integration" entry for the current enddate
        # 3. Apply /null/ → "effective" path fix on the serialized result
        log_timeoff_policytoassign = rail.PythonOperator(
            task_id='log_timeoff_policytoassign',
            python_callable=lambda dag_run: json.loads(
                json.dumps([
                    *[{
                        "effectiveDate": {
                            "day": str(entry['effectiveDate']['day']),
                            "month": str(entry['effectiveDate']['month']),
                            "year": str(entry['effectiveDate']['year'])
                        },
                        "policySet": {
                            **{k: v for k, v in entry['policySet'].items() if k not in ['timeOffValidationScripts', 'timeOffBalanceEventScripts']},
                            "timeOffValidationScripts": [
                                {**{k2: v2 for k2, v2 in s.items() if k2 != 'script'}, "scriptTarget": s.get('scriptTarget') or s.get('script')}
                                for s in entry['policySet'].get('timeOffValidationScripts', [])
                            ],
                            "timeOffBalanceEventScripts": [
                                {**{k2: v2 for k2, v2 in s.items() if k2 != 'script'}, "scriptTarget": s.get('scriptTarget') or s.get('script')}
                                for s in entry['policySet'].get('timeOffBalanceEventScripts', [])
                            ]
                        },
                        "description": entry['description']
                    }
                    for entry in dag_run.conf["policyset"]
                    if (datetime.strptime(dag_run.conf["enddate"], '%m/%d/%Y') -
                        datetime.strptime(
                            str(entry['effectiveDate']['day']) + "/" +
                            str(entry['effectiveDate']['month']) + "/" +
                            str(entry['effectiveDate']['year']),
                            '%m/%d/%Y'
                        )).days > 0
                    ],
                    {
                        "policySet": {
                            "timeOffBalanceEventScripts": [{
                                "additionalParameters": [{
                                    "keyUri": "urn:replicon:script-key:parameter:amount",
                                    "value": {"number": dag_run.conf["newschedulebalance"]}
                                }],
                                "scriptTarget": {
                                    "description": "Set initial balance for the first day of a policy",
                                    "name": "Starting Balance Set To",
                                    "uri": rail.result('get_all_scripts_time_off_balance_event_script')
                                }
                            }],
                            "timeOffValidationScripts": [{
                                "additionalParameters": [{
                                    "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                    "value": {"number": "0"}
                                }],
                                "scriptTarget": {
                                    "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                                    "name": "Prevent balance overdraw",
                                    "uri": rail.result('get_all_scripts_time_off_validation_script')
                                }
                            }]
                        },
                        "description": "Updated by Integration",
                        "effectiveDate": {
                            "day": dag_run.conf["enddate"].split("/")[1],
                            "month": dag_run.conf["enddate"].split("/")[0],
                            "year": dag_run.conf["enddate"].split("/")[2]
                        }
                    }
                ]).replace("/null/", '"effective"')
            )
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": dag_run.conf["timeoffuri"]
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policytoassign')
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "username": "",
                "userloginname": dag_run.conf.get('userloginname', ''),
                "action": "Timeoff Policy Update",
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        # ── Wiring ──────────────────────────────────────────────────────
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_all_scripts_time_off_validation_script
        get_all_scripts_time_off_validation_script >> get_all_scripts_time_off_balance_event_script >> log_timeoff_policytoassign
        log_timeoff_policytoassign >> put_user_time_off_account_policy_set_schedule >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
