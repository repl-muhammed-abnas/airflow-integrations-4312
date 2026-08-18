# pylint: disable=too-many-statements
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from momentive.user_import_south_korea.utils import request_payload, python_callable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'momentive_userimport_timeoff_add_newuser_child_{config.instance}',
        description=f'momentive_userimport_timeoff_add_newuser_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.timeoff_add_newuser_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_alltimeoff_types'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_alltimeoff_types',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        get_years_of_service = rail.PythonOperator(
            task_id='get_years_of_service',
            python_callable=lambda dag_run: ((datetime.now() - datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d')).days)/365
        )

        get_final_set_timeoff_15 = rail.PythonOperator(
            task_id = "get_final_set_timeoff_15",
            python_callable=python_callable.get_final_timeoff_newuser
        )

        assign_req_timeofftypes_16 = rail.RepliconServiceOperator(
            task_id='assign_req_timeofftypes_16',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'timeOffTypeUris': rail.result('get_final_set_timeoff_15')["final_timeoff_assign_val"]
            }
        )

        foreach_timeoffuri = rail.ForEachOperator(
            task_id='foreach_timeoffuri',
            items=lambda: json.loads(rail.result('get_final_set_timeoff_15')['final_timeoff_list']),
            start_task='if_uri_present',
            end_task='foreach_timeoffuri_end'
        )

        if_uri_present = rail.IfOperator(
            task_id='if_uri_present',
            test="{{ result('foreach_timeoffuri').uri | is_truthy }}",
            yes_task="if_timeoffname_not_KOR_monthly_or_annual",
            no_task="foreach_timeoffuri_end",
        )

        if_timeoffname_not_KOR_monthly_or_annual = rail.IfOperator(
            task_id='if_timeoffname_not_KOR_monthly_or_annual',
            test="{{ result('foreach_timeoffuri').name != 'KOR_Monthly Leave 월차휴가' and \
                result('foreach_timeoffuri').name != 'KOR_Annual Leave 연차휴가'}}",
            yes_task="if_timeoffname_is_bel",
            no_task="if_timeoffname_is_KOR_annual",
        )

        if_timeoffname_is_bel = rail.IfOperator(
            task_id='if_timeoffname_is_bel',
            test="{{ result('foreach_timeoffuri').name == '[Bel] ADV' or \
                result('foreach_timeoffuri').name == '[Bel] Jaarlijkse vakantie / Annual leave'}}",
            yes_task="if_startdate_is_01_01",
            no_task="if_timeoffname_is_UK_holiday",
        )

        if_startdate_is_01_01 = rail.IfOperator(
            task_id='if_startdate_is_01_01',
            test=lambda dag_run: bool(datetime.strptime(dag_run.conf['startdate'],'%Y-%m-%d').month == 1 and
                                    datetime.strptime(dag_run.conf['startdate'],'%Y-%m-%d').day == 1),
            yes_task="create_policylist_23",
            no_task="get_default_time_off_type_policy_schedule_for_user_37",
        )

        create_policylist_23 = rail.SetVariableOperator(
            task_id='create_policylist_23',
            append=False,
            name='policylist',
            value=[]
        )

        get_default_time_off_type_policy_schedule_for_user_24 = rail.RepliconServiceOperator(
            task_id="get_default_time_off_type_policy_schedule_for_user_24",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeofftype_policy_sched_payload
        )

        log_startbal_per_calendarmonth_26 = rail.PythonOperator(
            task_id='log_startbal_per_calendarmonth_26',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_24')[0]['policySet'],
                'script.name', "Starting Balance Per Calendar Month", "timeOffBalanceEventScripts.additionalParameters")).replace(
                    "[[", "[").replace("]]", "]")
        )

        log_bal_set_for_jan_28 = rail.PythonOperator(
            task_id='log_bal_set_for_jan_28',
            python_callable=lambda:  float(rail.find_first_by_attr_and_get_attr(rail.result(
                'log_startbal_per_calendarmonth_26'), 'keyUri', 'urn:replicon:script-key:parameter:january', 'value.number', ''))
        )

        log_existingbal_29 = rail.PythonOperator(
            task_id='log_existingbal_29',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:january",
                "value": {
                    "number": rail.result('log_bal_set_for_jan_28')
                }
            }, ensure_ascii=False)
        )

        log_newbal_if_startdate_is_jan_1_30 = rail.PythonOperator(
            task_id='log_newbal_if_startdate_is_jan_1_30',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:january",
                "value": {
                    "number": 0
                }
            }, ensure_ascii=False)
        )

        log_new_policy_to_assign_newuser_31 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_newuser_31',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_24')[0]['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"').replace(
                            rail.result('log_existingbal_29'),rail.result('log_newbal_if_startdate_is_jan_1_30')).replace(
                                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace(']}]', ']}'))
        )

        add_to_policy_list_newuser_33 = rail.SetVariableOperator(
            task_id='add_to_policy_list_newuser_33',
            append=True,
            name='{{ result("create_policylist_23").name }}',
            value=request_payload.add_to_policy_newuser_33
        )

        final_policy_newuser_34 = rail.PythonOperator(
            task_id='final_policy_newuser_34',
            python_callable=lambda: json.loads(json.dumps(rail.get_dag_run_var(rail.result('create_policylist_23')['name'])).replace(
                '"policySet":[', '"policySet":').replace("}}]}]", "}}]}").replace(
                    'timeOffValidationScripts":[]}]}]', 'timeOffValidationScripts":[]}}]'))
        )

        put_user_time_off_account_policy_set_schedule_35 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_35',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeoffuri')['uri']
                },
                "policySetScheduleEntries": rail.result('final_policy_newuser_34')
            }
        )

        get_default_time_off_type_policy_schedule_for_user_37 = rail.RepliconServiceOperator(
            task_id="get_default_time_off_type_policy_schedule_for_user_37",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeofftype_policy_sched_payload
        )

        if_default_policy_present = rail.IfOperator(
            task_id='if_default_policy_present',
            test="{{ result('get_default_time_off_type_policy_schedule_for_user_37') | is_truthy }}",
            yes_task="log_new_policy_to_assign_newuser_40",
            no_task="foreach_timeoffuri_end",
        )

        log_new_policy_to_assign_newuser_40 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_newuser_40',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_37'), ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_41 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_41',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeoffuri')['uri']
                },
                "policySetScheduleEntries": rail.result('log_new_policy_to_assign_newuser_40')
            }
        )

        if_timeoffname_is_UK_holiday = rail.IfOperator(
            task_id='if_timeoffname_is_UK_holiday',
            test="{{ result('foreach_timeoffuri').name == 'UK_Holiday Paid'}}",
            yes_task="create_policylist_44",
            no_task="get_default_time_off_type_policy_schedule_for_user_64",
        )

        create_policylist_44 = rail.SetVariableOperator(
            task_id='create_policylist_44',
            append=False,
            name='policylist_44',
            value=[]
        )

        get_default_time_off_type_policy_schedule_for_user_45 = rail.RepliconServiceOperator(
            task_id="get_default_time_off_type_policy_schedule_for_user_45",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeofftype_policy_sched_payload
        )

        log_yearly_accrual_47 = rail.PythonOperator(
            task_id='log_yearly_accrual_47',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_45')[0]['policySet'],
                'script.name', "Yearly Accrual", "timeOffBalanceEventScripts.additionalParameters")).replace("[[", "[").replace("]]", "]")
        )

        log_existing_accrual_balance_48 = rail.PythonOperator(
            task_id='log_existing_accrual_balance_48',
            python_callable=lambda:  float(rail.find_first_by_attr_and_get_attr(rail.result(
                'log_yearly_accrual_47'), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number', ''))
        )

        log_startbal_setto_script_50 = rail.PythonOperator(
            task_id='log_startbal_setto_script_50',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_45')[0]['policySet'],
                'script.name', "Starting Balance Set To", "timeOffBalanceEventScripts.additionalParameters")).replace("[[", "[").replace("]]", "]")
        )

        log_start_bal_val_52 = rail.PythonOperator(
            task_id='log_start_bal_val_52',
            python_callable=lambda:  float(rail.find_first_by_attr_and_get_attr(rail.result(
                'log_startbal_setto_script_50'), 'keyUri', 'urn:replicon:script-key:parameter:amount', 'value.number', ''))
        )

        log_existingstartbal_53 = rail.PythonOperator(
            task_id='log_existingstartbal_53',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                    "number": rail.result('log_start_bal_val_52')
                }
            }, ensure_ascii=False)
        )

        log_numberofdaysforproration_54 = rail.PythonOperator(
            task_id='log_numberofdaysforproration_54',
            python_callable=python_callable.get_number_of_days_proration
        )

        log_required_start_bal = rail.PythonOperator(
            task_id='log_required_start_bal',
            python_callable=python_callable.get_req_start_bal
        )

        log_newstartbal_57 = rail.PythonOperator(
            task_id='log_newstartbal_57',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                    "number": rail.result('log_required_start_bal')
                }
            }, ensure_ascii=False)
        )

        log_new_policy_to_assign_newuser_58 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_newuser_58',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_45')[0]['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"').replace(
                            rail.result('log_existingstartbal_53'),rail.result('log_newstartbal_57')).replace(
                                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace(']}]', ']}'))
        )

        add_to_policy_list_newuser_60 = rail.SetVariableOperator(
            task_id='add_to_policy_list_newuser_60',
            append=True,
            name='{{ result("create_policylist_44").name }}',
            value=request_payload.add_to_policy_newuser_60
        )

        final_policy_newuser_61 = rail.PythonOperator(
            task_id='final_policy_newuser_61',
            python_callable=lambda: json.loads(json.dumps(rail.get_dag_run_var(rail.result('create_policylist_44')['name'])).replace(
                '"policySet":[', '"policySet":').replace("}}]}]", "}}]}").replace(
                    'timeOffValidationScripts":[]}]}]', 'timeOffValidationScripts":[]}}]'))
        )

        put_user_time_off_account_policy_set_schedule_62 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_62',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeoffuri')['uri']
                },
                "policySetScheduleEntries": rail.result('final_policy_newuser_61')
            }
        )

        get_default_time_off_type_policy_schedule_for_user_64 = rail.RepliconServiceOperator(
            task_id="get_default_time_off_type_policy_schedule_for_user_64",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeofftype_policy_sched_payload
        )

        if_default_policy_present_65 = rail.IfOperator(
            task_id='if_default_policy_present_65',
            test="{{ result('get_default_time_off_type_policy_schedule_for_user_64') | is_truthy }}",
            yes_task="log_new_policy_to_assign_newuser_67",
            no_task="foreach_timeoffuri_end",
        )

        log_new_policy_to_assign_newuser_67 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_newuser_67',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_64'), ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_68 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_68',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeoffuri')['uri']
                },
                "policySetScheduleEntries": rail.result('log_new_policy_to_assign_newuser_67')
            }
        )

        if_timeoffname_is_KOR_annual = rail.IfOperator(
            task_id='if_timeoffname_is_KOR_annual',
            test="{{ result('foreach_timeoffuri').name == 'KOR_Annual Leave 연차휴가'}}",
            yes_task="get_calendar_years_of_service",
            no_task="if_timeoffname_is_KOR_monthly",
        )

        get_calendar_years_of_service = rail.PythonOperator(
            task_id='get_calendar_years_of_service',
            python_callable=lambda dag_run: datetime.now().year - datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').year
        )

        create_policylist_75=rail.SetVariableOperator(
            task_id='create_policylist_75',
            append=False,
            name='policylist_75',
            value=[]
        )

        create_yearlyentitilement=rail.SetVariableOperator(
            task_id='create_yearlyentitilement',
            append=False,
            name='yearlyentitilement',
            value=15
        )

        get_default_time_off_type_policy_schedule_for_user_77 = rail.RepliconServiceOperator(
            task_id="get_default_time_off_type_policy_schedule_for_user_77",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeofftype_policy_sched_payload
        )

        log_yearly_accrual_newuser_79 = rail.PythonOperator(
            task_id='log_yearly_accrual_newuser_79',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_77')[0]['policySet'],
                'script.name', "Yearly Accrual", "timeOffBalanceEventScripts.additionalParameters")).replace("[[", "[").replace("]]", "]")
        )

        log_existing_accrual_balance_newuser_81 = rail.PythonOperator(
            task_id='log_existing_accrual_balance_newuser_81',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'log_yearly_accrual_newuser_79'), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number', '')
        )

        log_existingaccrual_newuser_82 = rail.PythonOperator(
            task_id='log_existingaccrual_newuser_82',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('log_existing_accrual_balance_newuser_81')
                }
            }, ensure_ascii=False)
        )

        if_calendar_yos_less_than_2 = rail.IfOperator(
            task_id='if_calendar_yos_less_than_2',
            test=lambda: bool(rail.result('get_calendar_years_of_service') < 2 ),
            yes_task="log_numberofdaysforproration_for_yearly",
            no_task="if_calendar_yos_more_than_1_99",
        )

        log_numberofdaysforproration_for_yearly = rail.PythonOperator(
            task_id='log_numberofdaysforproration_for_yearly',
            python_callable=python_callable.get_number_of_days_proration
        )

        update_yearlyentitilement_85 = rail.SetVariableOperator(
            task_id='update_yearlyentitilement_85',
            append=False,
            name='{{ result("create_yearlyentitilement").name }}',
            value=python_callable.update_yearlyentitilement_val_30
        )

        if_calendar_yos_more_than_1_99 = rail.IfOperator(
            task_id='if_calendar_yos_more_than_1_99',
            test=lambda: bool(rail.result('get_calendar_years_of_service') > 1.99 ),
            yes_task="update_yearlyentitilement_87",
            no_task="log_accruals_rounded_value_89",
        )

        update_yearlyentitilement_87 = rail.SetVariableOperator(
            task_id='update_yearlyentitilement_87',
            append=False,
            name='{{ result("create_yearlyentitilement").name }}',
            value=lambda: round(((int(rail.result('get_calendar_years_of_service')) - 2) + float(rail.get_dag_run_var(
                rail.result('create_yearlyentitilement')['name']))), 2)
        )

        log_accruals_rounded_value_89 = rail.PythonOperator(
            task_id='log_accruals_rounded_value_89',
            python_callable=python_callable.accurals_rounded_val
        )

        log_newaccrual_90 = rail.PythonOperator(
            task_id='log_newaccrual_90',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('log_accruals_rounded_value_89')
                }
            }, ensure_ascii=False)
        )

        log_new_policy_to_assign_91 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_91',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_77')[0]['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"').replace(
                            rail.result('log_existingaccrual_newuser_82'),rail.result('log_newaccrual_90')).replace(
                                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace(']}]', ']}'))
        )

        add_to_policy_list_93 = rail.SetVariableOperator(
            task_id='add_to_policy_list_93',
            append=True,
            name='{{ result("create_policylist_75").name }}',
            value=request_payload.add_to_policy_newuser_93
        )

        get_policylist_variable = rail.GetVariableOperator(
            task_id='get_policylist_variable',
            name='policylist_75'
        )

        log_policy_94 = rail.PythonOperator(
            task_id='log_policy_94',
            python_callable=lambda: json.loads(json.dumps(
                rail.result('get_policylist_variable')['value'], ensure_ascii=False).replace(
                '"policySet":[', '"policySet":').replace("}}]}]", "}}]}").replace(
                    'timeOffValidationScripts":[]}]}]', 'timeOffValidationScripts":[]}}]')) if rail.result(
                        'get_policylist_variable')['value'] else ''
        )

        put_user_time_off_account_policy_set_schedule_95 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_95',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeoffuri')['uri']
                },
                "policySetScheduleEntries": rail.result('log_policy_94')
            }
        )

        if_timeoffname_is_KOR_monthly = rail.IfOperator(
            task_id='if_timeoffname_is_KOR_monthly',
            test="{{ result('foreach_timeoffuri').name == 'KOR_Monthly Leave 월차휴가'}}",
            yes_task="get_timeoffbalance_event_script_administration_service",
            no_task="foreach_timeoffuri_end",
        )

        get_timeoffbalance_event_script_administration_service = rail.RepliconServiceOperator(
            task_id='get_timeoffbalance_event_script_administration_service',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: {
                'startring_balance' : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Starting Balance Set To', 'uri', '')
            }
        )

        put_user_time_off_account_policy_set_schedule_99 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_99',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeoffuri')['uri']
                },
                "policySetScheduleEntries": ''
            }
        )

        foreach_timeoffuri_end = rail.EmptyOperator(
            task_id='foreach_timeoffuri_end',
        )

        if_timeofftype_startswith_UAE = rail.IfOperator(
            task_id='if_timeofftype_startswith_UAE',
            test=lambda dag_run: bool(dag_run.conf['timeofftypes'].startswith('UAE')),
            yes_task="UAE_timeoffs",
            no_task="catch_and_log_error",
        )

        UAE_timeoffs = rail.PythonOperator(
            task_id='UAE_timeoffs',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_alltimeoff_types'), 'displayText', 'UAE_Leave', 'uri', '')
        )

        assign_req_timeofftypes_158 = rail.RepliconServiceOperator(
            task_id='assign_req_timeofftypes_158',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'timeOffTypeUris': ["{{ result('UAE_timeoffs') }}"]
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "details":"{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_alltimeoff_types

        get_alltimeoff_types >> get_years_of_service >> get_final_set_timeoff_15 >> assign_req_timeofftypes_16 >> foreach_timeoffuri

        foreach_timeoffuri >> if_uri_present

        if_uri_present >> rail.Label('Yes') >> if_timeoffname_not_KOR_monthly_or_annual
        if_uri_present >> rail.Label('No') >> foreach_timeoffuri_end

        if_timeoffname_not_KOR_monthly_or_annual >> rail.Label('Yes') >> if_timeoffname_is_bel
        if_timeoffname_not_KOR_monthly_or_annual >> rail.Label('No') >> if_timeoffname_is_KOR_annual

        if_timeoffname_is_bel >> rail.Label('Yes') >> if_startdate_is_01_01
        if_timeoffname_is_bel >> rail.Label('No') >> if_timeoffname_is_UK_holiday

        if_startdate_is_01_01 >> rail.Label('Yes') >> create_policylist_23 >> get_default_time_off_type_policy_schedule_for_user_24 >> \
            log_startbal_per_calendarmonth_26 >> log_bal_set_for_jan_28 >> log_existingbal_29 >> log_newbal_if_startdate_is_jan_1_30 >> \
                log_new_policy_to_assign_newuser_31 >> add_to_policy_list_newuser_33 >> final_policy_newuser_34 >> \
                    put_user_time_off_account_policy_set_schedule_35 >> foreach_timeoffuri_end
        if_startdate_is_01_01 >> rail.Label('No') >> get_default_time_off_type_policy_schedule_for_user_37 >> if_default_policy_present

        if_default_policy_present >> rail.Label('Yes') >> log_new_policy_to_assign_newuser_40 >> \
            put_user_time_off_account_policy_set_schedule_41 >> foreach_timeoffuri_end
        if_default_policy_present >> rail.Label('No') >> foreach_timeoffuri_end

        if_timeoffname_is_UK_holiday >> rail.Label('Yes') >> create_policylist_44 >> get_default_time_off_type_policy_schedule_for_user_45 >> \
            log_yearly_accrual_47 >> log_existing_accrual_balance_48 >> log_startbal_setto_script_50 >> log_start_bal_val_52 >> \
                log_existingstartbal_53 >> log_numberofdaysforproration_54 >> log_required_start_bal >> log_newstartbal_57 >> \
                    log_new_policy_to_assign_newuser_58 >> add_to_policy_list_newuser_60 >> final_policy_newuser_61 >> \
                        put_user_time_off_account_policy_set_schedule_62 >> foreach_timeoffuri_end
        if_timeoffname_is_UK_holiday >> rail.Label('No') >> get_default_time_off_type_policy_schedule_for_user_64 >> if_default_policy_present_65

        if_default_policy_present_65 >> rail.Label('Yes') >> log_new_policy_to_assign_newuser_67 >> \
            put_user_time_off_account_policy_set_schedule_68 >> foreach_timeoffuri_end
        if_default_policy_present_65 >> rail.Label('No') >> foreach_timeoffuri_end

        if_timeoffname_is_KOR_annual >> rail.Label('Yes') >> get_calendar_years_of_service >> create_policylist_75 >> create_yearlyentitilement >> \
            get_default_time_off_type_policy_schedule_for_user_77 >> log_yearly_accrual_newuser_79 >> \
                log_existing_accrual_balance_newuser_81 >> log_existingaccrual_newuser_82 >> if_calendar_yos_less_than_2

        if_timeoffname_is_KOR_annual >> rail.Label('No') >> if_timeoffname_is_KOR_monthly

        if_calendar_yos_less_than_2 >> rail.Label('Yes') >> log_numberofdaysforproration_for_yearly >> update_yearlyentitilement_85 >> \
            if_calendar_yos_more_than_1_99
        if_calendar_yos_less_than_2 >> rail.Label('No') >> if_calendar_yos_more_than_1_99

        if_calendar_yos_more_than_1_99 >> rail.Label('Yes') >> update_yearlyentitilement_87 >> log_accruals_rounded_value_89
        if_calendar_yos_more_than_1_99 >> rail.Label('No') >> log_accruals_rounded_value_89

        log_accruals_rounded_value_89 >> log_newaccrual_90 >> log_new_policy_to_assign_91 >> add_to_policy_list_93 >> get_policylist_variable >> \
            log_policy_94 >> put_user_time_off_account_policy_set_schedule_95 >> if_timeoffname_is_KOR_monthly

        if_timeoffname_is_KOR_monthly >> rail.Label('Yes') >> get_timeoffbalance_event_script_administration_service >> \
            put_user_time_off_account_policy_set_schedule_99 >> foreach_timeoffuri_end
        if_timeoffname_is_KOR_monthly >> rail.Label('No') >> foreach_timeoffuri_end

        foreach_timeoffuri >> foreach_timeoffuri_end >> if_timeofftype_startswith_UAE

        if_timeofftype_startswith_UAE >> rail.Label('Yes') >> UAE_timeoffs >> assign_req_timeofftypes_158 >> catch_and_log_error
        if_timeofftype_startswith_UAE >> rail.Label('No') >> catch_and_log_error

        catch_and_log_error

        catch_and_log_error >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
