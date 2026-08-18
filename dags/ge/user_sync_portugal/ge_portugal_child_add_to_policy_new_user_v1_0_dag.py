
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_portugal_child_add_to_policy_new_user_v1_0_{config.instance}',
        description=f'GE_portugal_Child Workflow to add timeoff policy for new user v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            no_task='if_timeoff_not_equal_validation'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_timeoff_not_equal_validation',
            end_task='add_timeoff_type_logs_23',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_timeoff_not_equal_validation = rail.IfOperator(
            task_id='if_timeoff_not_equal_validation',
            test='''{{ dag_run.conf.name != '01_PT_Vacation/Férias' }}''',
            yes_task="get_default_time_off_type_policy_schedule_for_user_18",
            no_task="log_second_year_entitlement",
        )

        get_default_time_off_type_policy_schedule_for_user_18 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_18',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeofftypeuri }}"
                }
            }
        )

        log_timeoff_policy_20 = rail.PythonOperator(
            task_id='log_timeoff_policy_20',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_18'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"')) if rail.result('get_default_time_off_type_policy_schedule_for_user_18') else None
        )

        if_log_timeoff_policy_20_present_21 = rail.IfOperator(
            task_id='if_log_timeoff_policy_20_present_21',
            test='''{{ result('log_timeoff_policy_20') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_22",
            no_task="add_timeoff_type_logs_23",
        )

        put_user_time_off_account_policy_set_schedule_22 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_22',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeofftypeuri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_20')
            }
        )

        def get_second_year_entitlement(dag_run):
            second_year_entitlement = 24
            if dag_run.conf['legalentity'] and dag_run.conf['legalentity'] in ['I00435', 'MS0208', 'MS0218']:
                second_year_entitlement = 25
            return second_year_entitlement

        log_second_year_entitlement = rail.PythonOperator(
            task_id='log_second_year_entitlement',
            python_callable=get_second_year_entitlement
        )

        get_all_scriptsfor_time_off_balance_event_script_administration_service = rail.RepliconServiceOperator(
            task_id='get_all_scriptsfor_time_off_balance_event_script_administration_service',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts"
        )

        log_monthly_accrual_script_target = rail.PythonOperator(
            task_id='log_monthly_accrual_script_target',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scriptsfor_time_off_balance_event_script_administration_service'), 'displayText', 'Monthly Accrual', 'uri')
        )

        log_yearly_accrual_script_target = rail.PythonOperator(
            task_id='log_yearly_accrual_script_target',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scriptsfor_time_off_balance_event_script_administration_service'), 'displayText', 'Yearly Accrual', 'uri')
        )

        log_yearly_carryover_script_target = rail.PythonOperator(
            task_id='log_yearly_carryover_script_target',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scriptsfor_time_off_balance_event_script_administration_service'), 'displayText', 'Yearly Carry Over with Expiry', 'uri')
        )

        def get_to_policy_schedule(dag_run):
            start_date = datetime.strptime(
                dag_run.conf['startdate'], '%d/%m/%Y')
            start_date_12 = start_date + relativedelta(months=+12)
            start_of_year_12 = start_date_12.replace(month=1, day=1)
            previous_year_date = start_of_year_12 + timedelta(days=-1)
            carryupto_month_joining = 13 - start_date.month
            carryupto_amount_month_joining = carryupto_month_joining * 2
            differance_in_days = (previous_year_date - start_date).days
            if differance_in_days > 179:
                return [
                    {
                        "effectiveDate": {
                            "year": start_date.year,
                            "month": start_date.month,
                            "day": start_date.day
                        },
                        "description": "Effective On " + str(start_date.year) + "-" + str(start_date.month)+"-" + str(start_date.day),
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.result('log_monthly_accrual_script_target')
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                            "value": {
                                                "number": 24
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                                            "value": {
                                                "uri": "urn:replicon:monthly-frequency-start-day-option:last-day-of-month"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:proration-option",
                                            "value": {
                                                "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "number": "30"
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": []
                        }
                    },
                    {
                        "effectiveDate": {
                            "year": start_of_year_12.year,
                            "month": start_of_year_12.month,
                            "day": start_of_year_12.day
                        },
                        "description": "Effective On 2019-01-01",
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.result('log_yearly_accrual_script_target')
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                            "value": {
                                                "number": rail.result('log_second_year_entitlement')
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                                            "value": {
                                                "uri": "urn:replicon:month:january"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                                            "value": {
                                                "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:proration-option",
                                            "value": {
                                                "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "number": "30"
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": []
                        }
                    }
                ]

            return [
                {
                    "effectiveDate": {
                        "year": start_date.year,
                        "month": start_date.month,
                        "day": start_date.day
                    },
                    "description": "Effective On " + str(start_date.month) + "-" + str(start_date.day)+"-" + str(start_date.year),
                    "policySet": {
                        "timeOffBalanceEventScripts": [
                            {
                                "scriptTarget": {
                                    "uri": rail.result('log_monthly_accrual_script_target')
                                },
                                "additionalParameters": [
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                        "value": {
                                            "number": 24
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                                        "value": {
                                            "uri": "urn:replicon:monthly-frequency-start-day-option:last-day-of-month"
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:proration-option",
                                        "value": {
                                            "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:precedence",
                                        "value": {
                                            "number": "30"
                                        }
                                    }
                                ]
                            },
                            {
                                "scriptTarget": {
                                    "uri": rail.result('log_yearly_carryover_script_target')
                                },
                                "additionalParameters": [
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:reset-on-month",
                                        "value": {
                                            "uri": "urn:replicon:month:january"
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                                        "value": {
                                            "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:carry-up-to-amount",
                                        "value": {
                                            "number": carryupto_amount_month_joining
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:expire-after",
                                        "value": {
                                            "number": 181
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:expire-after-unit",
                                        "value": {
                                            "uri": "urn:replicon:time-off-expire-after-unit:days"
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
                                            "number": "20"
                                        }
                                    }
                                ]
                            }
                        ],
                        "timeOffValidationScripts": []
                    }
                },
                {
                    "effectiveDate": {
                        "year": start_of_year_12.year,
                        "month": start_of_year_12.month,
                        "day": start_of_year_12.day
                    },
                    "description": "Effective On " + str(start_of_year_12.month) + "-" + str(start_of_year_12.day)+"-" + str(start_of_year_12.year),
                    "policySet": {
                        "timeOffBalanceEventScripts": [
                            {
                                "scriptTarget": {
                                    "uri": rail.result('log_yearly_accrual_script_target')
                                },
                                "additionalParameters": [
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                        "value": {
                                            "number": "24"
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                                        "value": {
                                            "uri": "urn:replicon:month:january"
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                                        "value": {
                                            "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:proration-option",
                                        "value": {
                                            "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                                        }
                                    },
                                    {
                                        "keyUri": "urn:replicon:script-key:parameter:precedence",
                                        "value": {
                                            "number": "30"
                                        }
                                    }
                                ]
                            }
                        ],
                        "timeOffValidationScripts": []
                    }
                }
            ]

        put_user_time_off_account_policy_set_schedule_45 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_45',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeofftypeuri'],
                },
                "policySetScheduleEntries": get_to_policy_schedule(dag_run)
            }
        )

        add_timeoff_type_logs_23 = rail.WriteLogOperator(
            task_id='add_timeoff_type_logs_23',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": ""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> add_timeoff_type_logs_23
        can_run_batch_task >> rail.Label(
            'No') >> if_timeoff_not_equal_validation
        if_timeoff_not_equal_validation >> rail.Label(
            'No') >> log_second_year_entitlement >> get_all_scriptsfor_time_off_balance_event_script_administration_service >> \
            log_monthly_accrual_script_target >> log_yearly_accrual_script_target >> log_yearly_carryover_script_target >> \
            put_user_time_off_account_policy_set_schedule_45 >> add_timeoff_type_logs_23
        if_timeoff_not_equal_validation >> rail.Label('Yes') >> get_default_time_off_type_policy_schedule_for_user_18 >> \
            log_timeoff_policy_20 >> if_log_timeoff_policy_20_present_21
        if_log_timeoff_policy_20_present_21 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_22 >> add_timeoff_type_logs_23
        if_log_timeoff_policy_20_present_21 >> rail.Label(
            'No') >> add_timeoff_type_logs_23 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
