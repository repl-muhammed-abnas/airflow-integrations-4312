
from datetime import timedelta, datetime
import json
import pendulum
from airflow.models import Variable
from ge_healthcare.user_sync_denmark.denmark_master_mapper import denmark_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_denmark_child_add_to_policy_new_user_v1_0_{config.instance}',
        description=f'GE_denmark_Child Workflow to add timeoff policy for new user v1.0 {config.instance}',
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
            no_task='get_default_time_off_type_policy_schedule_for_user_18'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_default_time_off_type_policy_schedule_for_user_18',
            end_task='add_timeoff_type_logs_23',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
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

        if_timeoff_not_equal_validation_28 = rail.IfOperator(
            task_id='if_timeoff_not_equal_validation_28',
            test='''{{ dag_run.conf.name != '01. DK_Vacation' and dag_run.conf.name != '02. DK_Feriefridage' }}''',
            yes_task="log_timeoff_policy_20",
            no_task="if_name_equals_to_01dk_vacation_33"
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

        if_name_equals_to_01dk_vacation_33 = rail.IfOperator(
            task_id='if_name_equals_to_01dk_vacation_33',
            test='''{{ dag_run.conf.name == '01. DK_Vacation' }}''',
            yes_task="if_request_full_part_equals_to_fulltime_34",
            no_task="ge_denmark_user_sync_master_mapper_v2_0_search_entries_41",
        )

        if_request_full_part_equals_to_fulltime_34 = rail.IfOperator(
            task_id='if_request_full_part_equals_to_fulltime_34',
            test='''{{ dag_run.conf.fullpart == 'Full Time' }}''',
            yes_task="log_timeoff_policy_35",
            no_task="trigger_dag_run_ge_user_sync_denmark_child_vacation_parttime_v1_039",
        )

        log_timeoff_policy_35 = rail.PythonOperator(
            task_id='log_timeoff_policy_35',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_18'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"')) if rail.result('get_default_time_off_type_policy_schedule_for_user_18') else None
        )

        if_log_timeoff_policy_35_present_36 = rail.IfOperator(
            task_id='if_log_timeoff_policy_35_present_36',
            test='''{{ result('log_timeoff_policy_35') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_37",
            no_task="add_timeoff_type_logs_23",
        )

        put_user_time_off_account_policy_set_schedule_37 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_37',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeofftypeuri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_35')
            }
        )

        trigger_dag_run_ge_user_sync_denmark_child_vacation_parttime_v1_039 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_denmark_child_vacation_parttime_v1_039',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'] if dag_run.conf['type'] == 'Add' else pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y'),
                "type": dag_run.conf['type'],
                "numberofworkingdays": dag_run.conf['numberofworkingdays'],
                "timeoffuri": dag_run.conf['timeofftypeuri']
            }
        )

        wait_for_completion_trigger_trigger_dag_run_ge_user_sync_denmark_child_vacation_parttime_v1_039 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_trigger_dag_run_ge_user_sync_denmark_child_vacation_parttime_v1_039',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_denmark_child_vacation_parttime_v1_039") }}'
        )

        dummy_operator_1 = rail.EmptyOperator(
            task_id="dummy_operator_1"
        )

        def get_entity_from_mapper(LegalEntity, to_name):
            mapperinfo = list(filter(
                lambda x: x['legal_entity'] == LegalEntity
                and x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == to_name, denmark_master_mapper))
            return [emp_info['value'] for emp_info in mapperinfo]

        ge_denmark_user_sync_master_mapper_v2_0_search_entries_41 = rail.PythonOperator(
            task_id='ge_denmark_user_sync_master_mapper_v2_0_search_entries_41',
            python_callable=lambda dag_run:  get_entity_from_mapper(
                dag_run.conf['LegalEntity'], dag_run.conf['name'])
        )

        if_first_id_present_42 = rail.IfOperator(
            task_id='if_first_id_present_42',
            test='''{{ result('ge_denmark_user_sync_master_mapper_v2_0_search_entries_41') | is_truthy }}''',
            yes_task="log_required_reset_balance_43",
            no_task="add_timeoff_type_logs_23",
        )

        def get_timeoff_info_from_mapper(LegalEntity, to_name, to_type):
            mapperinfo = list(filter(
                lambda x: x['legal_entity'] == LegalEntity
                and x['type'] == to_type
                and x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == to_name, denmark_master_mapper))
            return mapperinfo[0]['value'] if mapperinfo else None

        def get_timeoff_starting_balance_from_mapper(dag_run, to_type):
            LegalEntity = dag_run.conf['LegalEntity']
            to_name = dag_run.conf['name']
            start_date = datetime.strptime(
                dag_run.conf['startdate'], '%d/%m/%Y')
            start_date_month = start_date.strftime("%b")
            mapperinfo = list(filter(
                lambda x: x['legal_entity'] == LegalEntity
                and x['type'] == to_type
                and x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == to_name
                and x['identifier__2__(_legal_entity_name/_start_date_month)'] == start_date_month, denmark_master_mapper))
            return mapperinfo[0]['value'] if mapperinfo else None

        def get_timeoff_accrual_from_mapper(LegalEntity, to_name, to_type):
            mapperinfo = list(filter(
                lambda x: x['legal_entity'] == LegalEntity
                and x['type'] == to_type
                and x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == to_name, denmark_master_mapper))
            return mapperinfo[0]['default_uri'] if mapperinfo else None

        log_required_reset_balance_43 = rail.PythonOperator(
            task_id='log_required_reset_balance_43',
            python_callable=lambda dag_run: get_timeoff_info_from_mapper(
                dag_run.conf['LegalEntity'], dag_run.conf['name'], 'Timeoff Reset Balance')
        )

        log_required_starting_balance_44 = rail.PythonOperator(
            task_id='log_required_starting_balance_44',
            python_callable=lambda dag_run: get_timeoff_starting_balance_from_mapper(
                dag_run, 'Timeoff Starting Balance')
        )

        log_required_accrual_balance_45 = rail.PythonOperator(
            task_id='log_required_accrual_balance_45',
            python_callable=lambda dag_run: get_timeoff_info_from_mapper(
                dag_run.conf['LegalEntity'], dag_run.conf['name'], 'Time off accrual')
        )

        log_required_accrual_proration_46 = rail.PythonOperator(
            task_id='log_required_accrual_proration_46',
            python_callable=lambda dag_run: get_timeoff_accrual_from_mapper(
                dag_run.conf['LegalEntity'], dag_run.conf['name'], 'Timeoff proration')
        )

        log_required_accrual_day_47 = rail.PythonOperator(
            task_id='log_required_accrual_day_47',
            python_callable=lambda dag_run: get_timeoff_accrual_from_mapper(
                dag_run.conf['LegalEntity'], dag_run.conf['name'], 'Day of acccrual')
        )

        log_required_reset_monthand_day_48 = rail.PythonOperator(
            task_id='log_required_reset_monthand_day_48',
            python_callable=lambda dag_run: get_timeoff_info_from_mapper(
                dag_run.conf['LegalEntity'], dag_run.conf['name'], 'Timeoff Reset Date')
        )

        if_request_legalentity_equals_to_ps0259_59 = rail.IfOperator(
            task_id='if_request_legalentity_equals_to_ps0259_59',
            test='''{{ dag_run.conf.LegalEntity == 'PS0259' }}''',
            yes_task="log_policy_setup_60",
            no_task="log_policy_setup_63",
        )

        def get_policy_set_60(dag_run):
            reset_balance = int(rail.result('log_required_reset_balance_43')) if rail.result(
                'log_required_reset_balance_43') else 0
            rest_day_month = rail.result('log_required_reset_monthand_day_48')
            todays_Year = pendulum.now(config.pacific_timezone).year
            reset_date_string = rest_day_month + "-" + str(todays_Year)
            reset_date = datetime.strptime(reset_date_string, '%d-%b-%Y')
            reset_date_fullmonth = reset_date.strftime('%B')
            reset_date_month_uri = "urn:replicon:month:" + reset_date_fullmonth.lower()
            reset_date_day = reset_date.day
            reset_date_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                str(reset_date.day) + "th"
            if reset_date_day in [1, 21, 31]:
                reset_date_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                    str(reset_date.day) + "st"
            if reset_date_day in [2, 22]:
                reset_date_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                    str(reset_date.day) + "nd"
            if reset_date_day in [3, 23]:
                reset_date_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                    str(reset_date.day) + "rd"
            accrual_balance = int(rail.result('log_required_accrual_balance_45')) if rail.result(
                'log_required_accrual_balance_45') else 0
            accrue_day = rail.result('log_required_accrual_day_47')
            proration = rail.result('log_required_accrual_proration_46')

            return {
                "timeOffBalanceEventScripts": [
                    {
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:periodic-reset-option",
                                "value": {
                                    "uri": "urn:replicon:time-off-policy-reset-option:reset-balance-to-specific-value"
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:precedence",
                                "value": {
                                    "number": 20
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                                "value": {
                                    "number": reset_balance
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                                "value": {
                                    "uri": reset_date_day_uri
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:reset-on-month",
                                "value": {
                                    "uri": reset_date_month_uri
                                }
                            }
                        ],
                        "scriptTarget": {
                            "description": "Reset balance once a year",
                            "name": "Yearly Reset",
                            "uri": dag_run.conf['yearlyreseturi']
                        }
                    },
                    {
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": accrual_balance
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                                "value": {
                                    "uri": accrue_day
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
                                    "uri": proration
                                }
                            }
                        ],
                        "scriptTarget": {
                            "description": "Accrues time once per month.",
                            "name": "Monthly Accrual",
                            "uri": dag_run.conf['monthlyaccrualuri']
                        }
                    }
                ],
                "timeOffValidationScripts": []
            }

        log_policy_setup_60 = rail.PythonOperator(
            task_id='log_policy_setup_60',
            python_callable=get_policy_set_60
        )

        put_user_time_off_account_policy_set_schedule_61 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_61',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeofftypeuri']
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": {
                            "year": datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').year,
                            "month": datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').month,
                            "day": datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').day
                        },
                        "description": "Effective on" + dag_run.conf['startdate'],
                        "policySet": rail.result('log_policy_setup_60')
                    }
                ]
            }
        )

        def get_policy_set_61(dag_run):
            reset_balance = int(rail.result('log_required_reset_balance_43')) if rail.result(
                'log_required_reset_balance_43') else 0
            start_balance = int(rail.result('log_required_starting_balance_44')) if rail.result(
                'log_required_starting_balance_44') else 0
            rest_day_month = rail.result('log_required_reset_monthand_day_48')
            todays_Year = pendulum.now(config.pacific_timezone).year
            reset_date_string = rest_day_month + "-" + str(todays_Year)
            reset_date = datetime.strptime(reset_date_string, '%d-%b-%Y')
            reset_date_fullmonth = reset_date.strftime('%B')
            reset_date_month_uri = "urn:replicon:month:" + reset_date_fullmonth.lower()
            reset_date_day = reset_date.day
            reset_date_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                str(reset_date.day) + "th"
            if reset_date_day in [1, 21, 31]:
                reset_date_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                    str(reset_date.day) + "st"
            if reset_date_day in [2, 22]:
                reset_date_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                    str(reset_date.day) + "nd"
            if reset_date_day in [3, 23]:
                reset_date_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                    str(reset_date.day) + "rd"
            return {
                "timeOffBalanceEventScripts": [{
                    "additionalParameters": [
                        {
                            "keyUri": "urn:replicon:script-key:parameter:periodic-reset-option",
                            "value": {
                                "uri": "urn:replicon:time-off-policy-reset-option:reset-balance-to-specific-value"
                            }
                        },
                        {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                                "number": 20
                            }
                        },
                        {
                            "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                            "value": {
                                "number": reset_balance
                            }
                        },
                        {
                            "keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                            "value": {
                                "uri": reset_date_day_uri
                            }
                        },
                        {
                            "keyUri": "urn:replicon:script-key:parameter:reset-on-month",
                            "value": {
                                "uri": reset_date_month_uri
                            }
                        }
                    ],
                    "scriptTarget": {
                        "description": "Reset balance once a year",
                        "name": "Yearly Reset",
                        "uri": dag_run.conf['yearlyreseturi']
                    }
                },
                    {
                    "additionalParameters": [
                        {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "number": start_balance
                            }
                        },
                        {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                                "number": 10
                            }
                        }
                    ],
                    "scriptTarget": {
                        "description": "Set initial balance for the first day of a policy",
                        "name": "Starting Balance Set To",
                        "uri": dag_run.conf['startingbalanceuri']
                    }
                }
                ],
                "timeOffValidationScripts": []
            }

        log_policy_setup_63 = rail.PythonOperator(
            task_id='log_policy_setup_63',
            python_callable=get_policy_set_61
        )

        put_user_time_off_account_policy_set_schedule_64 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_64',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeofftypeuri']
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": {
                            "year": datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').year,
                            "month": datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').month,
                            "day": datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').day
                        },
                        "description": "Effective on" + dag_run.conf['startdate'],
                        "policySet": rail.result('log_policy_setup_63')
                    }
                ]
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
                "OHRID": "{{ dag_run.conf.userloginname }}",
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
            'No') >> get_default_time_off_type_policy_schedule_for_user_18 >> if_timeoff_not_equal_validation_28
        if_timeoff_not_equal_validation_28 >> rail.Label(
            'No') >> if_name_equals_to_01dk_vacation_33
        if_name_equals_to_01dk_vacation_33 >> rail.Label(
            'No') >> ge_denmark_user_sync_master_mapper_v2_0_search_entries_41 >> if_first_id_present_42
        if_name_equals_to_01dk_vacation_33 >> rail.Label(
            'Yes') >> if_request_full_part_equals_to_fulltime_34
        if_request_full_part_equals_to_fulltime_34 >> rail.Label('No') >> trigger_dag_run_ge_user_sync_denmark_child_vacation_parttime_v1_039 >> \
            wait_for_completion_trigger_trigger_dag_run_ge_user_sync_denmark_child_vacation_parttime_v1_039 >> \
            dummy_operator_1 >> add_timeoff_type_logs_23
        if_request_full_part_equals_to_fulltime_34 >> rail.Label(
            'Yes') >> log_timeoff_policy_35 >> if_log_timeoff_policy_35_present_36
        if_log_timeoff_policy_35_present_36 >> rail.Label(
            'Yes') >> add_timeoff_type_logs_23
        if_log_timeoff_policy_35_present_36 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_37 >> add_timeoff_type_logs_23
        if_first_id_present_42 >> rail.Label('No') >> add_timeoff_type_logs_23
        if_first_id_present_42 >> rail.Label('Yes') >> log_required_reset_balance_43 >> log_required_starting_balance_44 >> \
            log_required_accrual_balance_45 >> log_required_accrual_proration_46 >> log_required_accrual_day_47 >> \
            log_required_reset_monthand_day_48 >> if_request_legalentity_equals_to_ps0259_59
        if_request_legalentity_equals_to_ps0259_59 >> rail.Label(
            'No') >> log_policy_setup_63 >> put_user_time_off_account_policy_set_schedule_64 >> add_timeoff_type_logs_23
        if_request_legalentity_equals_to_ps0259_59 >> rail.Label(
            'Yes') >> log_policy_setup_60 >> put_user_time_off_account_policy_set_schedule_61 >> add_timeoff_type_logs_23
        if_timeoff_not_equal_validation_28 >> rail.Label('Yes') >> \
            log_timeoff_policy_20 >> if_log_timeoff_policy_20_present_21
        if_log_timeoff_policy_20_present_21 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_22 >> add_timeoff_type_logs_23
        if_log_timeoff_policy_20_present_21 >> rail.Label(
            'No') >> add_timeoff_type_logs_23 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
