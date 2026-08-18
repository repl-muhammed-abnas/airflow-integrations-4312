
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from ge_healthcare.user_sync_netherlands.netherlands_timeoff_mapper import netherlands_timeoff_mapper
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_netherlands_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_start_date_v1_0{config.instance}',
        description=f'GE Netherlands_Child Timeoff type Proration Assignment for 	04. NL_Seniority Days (Start Date) v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
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
            no_task='getalscriptsfor_time_off_balance_event_script_administration_service1_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='getalscriptsfor_time_off_balance_event_script_administration_service1_3',
            end_task='catch_16_16_16',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        getalscriptsfor_time_off_balance_event_script_administration_service1_3 = rail.RepliconServiceOperator(
            task_id='getalscriptsfor_time_off_balance_event_script_administration_service1_3',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data=None
        )

        log_yearly_accrualwith_expiry_script_4 = rail.PythonOperator(
            task_id='log_yearly_accrualwith_expiry_script_4',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getalscriptsfor_time_off_balance_event_script_administration_service1_3'), 'displayText', 'Yearly Accrual with Expiry', 'uri')
        )

        ge_netherlands_timeoff_mapper_search_entries_5 = rail.PythonOperator(
            task_id='ge_netherlands_timeoff_mapper_search_entries_5',
            python_callable=lambda dag_run:  list(filter(lambda x: x['timeoff_type_name'] == dag_run.conf['timeofftype']
                                                         and x['legacy_payroll_id_|_payrule'] == dag_run.conf['legacypayrollid'], netherlands_timeoff_mapper))
        )

        def get_policy_to_assign(dag_run):
            policy_to_assign = []
            timeoff_info_from_mapper = rail.result(
                'ge_netherlands_timeoff_mapper_search_entries_5')
            for toinfo in timeoff_info_from_mapper:
                actualhours = toinfo['accural_need_to_be_added_|_accrual'].split(
                    '|')[-1]
                topolicy = {
                    "timeOffBalanceEventScripts": [
                        {
                            "additionalParameters": [
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                    "value": {
                                        "number": actualhours
                                    }
                                },
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                                    "value": {
                                        "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                                    }
                                },
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                                    "value": {
                                        "uri": "urn:replicon:month:january"
                                    }
                                },
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:expire-after",
                                    "value": {
                                        "number": 6
                                    }
                                },
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:expire-after-unit",
                                    "value": {
                                        "uri": "urn:replicon:time-off-expire-after-unit:years"
                                    }
                                },
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:expiry-upon-option",
                                    "value": {
                                        "uri": "urn:replicon:time-off-upon-expiry-option:do-not-pay-out"
                                    }
                                },
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:precedence",
                                    "value": {
                                        "number": 30
                                    }
                                },
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:proration-option",
                                    "value": {
                                        "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                                    }
                                }
                            ],
                            "scriptTarget": {
                                "description": " Accrues time once per year with expiry",
                                "name": "Yearly Accrual with Expiry",
                                "uri": rail.result('log_yearly_accrualwith_expiry_script_4')
                            }
                        }
                    ],
                    "timeOffValidationScripts": []
                }

                offset = int(float(toinfo['offset']) * 12)
                effectivedate = datetime.strptime(
                    dag_run.conf["startdate"], "%d/%m/%Y") + relativedelta(months=offset)
                policy_to_assign.append({
                                        "description": "Effective from" + effectivedate.strftime("%Y-%m-%d"),
                                        "effectiveDate": {
                                            "day": effectivedate.day,
                                            "month": effectivedate.month,
                                            "year": effectivedate.year,
                                        },
                                        "policySet": topolicy
                                        })

            return policy_to_assign

        log_policy_to_assign_8 = rail.PythonOperator(
            task_id='log_policy_to_assign_8',
            python_callable=get_policy_to_assign
        )

        if_declare_list_6_list_items_greater_than_0_13 = rail.IfOperator(
            task_id='if_declare_list_6_list_items_greater_than_0_13',
            test='''{{ result('log_policy_to_assign_8') | length > 0 }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_15",
            no_task="catch_16_16_16",
        )

        put_user_time_off_account_policy_set_schedule_15 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_15',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_policy_to_assign_8')
            }
        )

        catch_16_16_16 = rail.EmptyOperator(
            task_id='catch_16_16_16',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_16_16_16
        can_run_batch_task >> rail.Label(
            'No') >> getalscriptsfor_time_off_balance_event_script_administration_service1_3
        getalscriptsfor_time_off_balance_event_script_administration_service1_3 >> \
            log_yearly_accrualwith_expiry_script_4 >> ge_netherlands_timeoff_mapper_search_entries_5 >> \
            log_policy_to_assign_8 >> if_declare_list_6_list_items_greater_than_0_13
        if_declare_list_6_list_items_greater_than_0_13 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_15 >> catch_16_16_16
        if_declare_list_6_list_items_greater_than_0_13 >> rail.Label(
            'No') >> catch_16_16_16 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
