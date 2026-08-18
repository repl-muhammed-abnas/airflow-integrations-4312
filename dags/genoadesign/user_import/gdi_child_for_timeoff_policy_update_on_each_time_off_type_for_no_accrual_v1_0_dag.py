
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'genoadesign_user_import_gdi_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_0_{config.instance}',
        description=f'Live|GDI_Child for timeoff policy update on each time off type for no accrual V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        max_active_runs=1,
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
            no_task='get_all_scripts_time_off_validation_script_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_scripts_time_off_validation_script_4',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_scripts_time_off_validation_script_4 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_validation_script_4',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data=None
        )

        get_all_scripts_time_off_balance_event_script_5 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_balance_event_script_5',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data=None
        )

        log_get_script_urifor_prevent_balance_overdraw_8 = rail.PythonOperator(
            task_id='log_get_script_urifor_prevent_balance_overdraw_8',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scripts_time_off_validation_script_4'), 'displayText', "Prevent balance overdraw", 'uri')
        )

        log_get_script_urifor_initial_balance_9 = rail.PythonOperator(
            task_id='log_get_script_urifor_initial_balance_9',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scripts_time_off_balance_event_script_5'), 'displayText', "Starting Balance Set To", 'uri')
        )

        def get_datetime_obj(effectiveDate):
            year = effectiveDate['year']
            month = effectiveDate['month']
            day = effectiveDate['day']
            return datetime.strptime(f"{year}/{month}/{day}", '%Y/%m/%d')

        def get_policy_set_schedule_entries(dag_run):
            input_policy = dag_run.conf['policyset']
            # input_policy = json.loads(dag_run.conf['policyset'])
            policy_set_schedule_entries = []
            for policy in input_policy:
                end_date = datetime.strptime(
                    dag_run.conf['enddate'], '%Y%m%d')
                effective_date = get_datetime_obj(policy['effectiveDate'])
                day_diff = (end_date - effective_date).days
                if day_diff > -1:
                    policy_set_schedule_entries.append({
                        "effectiveDate": policy['effectiveDate'],
                        "description": policy['description'],
                        "policySet": json.loads(json.dumps(policy['policySet'], ensure_ascii=False).replace('null', '"effective"').replace(
                            '"script"', '"scriptTarget"'))
                    })
            schedule_entries = {
                "policySet": {
                    "timeOffBalanceEventScripts": [
                        {
                            "additionalParameters": [
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:amount",
                                    "value": {"number": dag_run.conf['newschedulebalance']}
                                }
                            ],
                            "scriptTarget": {
                                # "description": "Set initial balance for the first day of a policy",
                                "name": "Starting Balance Set To",
                                "uri": rail.result('log_get_script_urifor_initial_balance_9')
                            }
                        }
                    ],
                    "timeOffValidationScripts": [
                        {
                            "additionalParameters": [
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                    "value": {
                                        "number": "0"
                                    }
                                }
                            ],
                            "scriptTarget": {
                                # "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                                "name": "Prevent balance overdraw",
                                "uri": rail.result('log_get_script_urifor_prevent_balance_overdraw_8')
                            }
                        }
                    ]
                },
                "description": "Updated by Integration",
                "effectiveDate": {
                    "day": datetime.strptime(dag_run.conf['enddate'], '%Y%m%d').day,
                    "month": datetime.strptime(dag_run.conf['enddate'], '%Y%m%d').month,
                    "year": datetime.strptime(dag_run.conf['enddate'], '%Y%m%d').year
                }
            }

            policy_set_schedule_entries.append(schedule_entries)
            return policy_set_schedule_entries

        put_user_time_off_account_policy_set_schedule_27 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_27',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": get_policy_set_schedule_entries(dag_run)
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_all_scripts_time_off_validation_script_4 >> \
            get_all_scripts_time_off_balance_event_script_5 >> \
            log_get_script_urifor_prevent_balance_overdraw_8 >> log_get_script_urifor_initial_balance_9 >> \
            put_user_time_off_account_policy_set_schedule_27 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
