# pylint: disable=too-many-statements line-too-long
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from momentive.common_recipes_userimport.utils import request_payload, python_callable

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.momentive_othercountries_user_sync_timeoff_rehire_user_child_dag_id,
        description=f'momentive_othercountries_user_sync_timeoff_add_newuser_rehire_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        get_years_of_service = rail.PythonOperator(
            task_id='get_years_of_service',
            python_callable=lambda dag_run: ((datetime.now() - datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d')).days)/365
        )

        get_calendar_years_of_service = rail.PythonOperator(
            task_id='get_calendar_years_of_service',
            python_callable=lambda dag_run: datetime.now().year - datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').year
        )

        if_timeoffuri_is_present = rail.IfOperator(
            task_id='if_timeoffuri_is_present',
            test="{{ dag_run.conf.timeoffuri | is_truthy}}",
            yes_task="if_timeofftype_not_KOR_monthlyleave_and_annualleave",
            no_task="final_response_from_dag",
        )

        if_timeofftype_not_KOR_monthlyleave_and_annualleave = rail.IfOperator(
            task_id='if_timeofftype_not_KOR_monthlyleave_and_annualleave',
            test="{{ dag_run.conf.timeofftypes != 'KOR_Monthly Leave 월차휴가' and \
                dag_run.conf.timeofftypes != 'KOR_Annual Leave 연차휴가' }}",
            yes_task="get_default_time_off_type_policy_schedule_for_user",
            no_task="if_timeofftype_equals_KOR_annualleave",
        )

        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id="get_default_time_off_type_policy_schedule_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeofftype_policy_sched_payload,
            data_handler=python_callable.get_policy_to_assign
        )

        is_policy_present = rail.IfOperator(
            task_id='is_policy_present',
            test=lambda: bool(rail.result('get_default_time_off_type_policy_schedule_for_user')),
            yes_task='put_user_timeoff_policy',
            no_task='final_response_from_dag'
        )

        put_user_timeoff_policy = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_user_timeoff_policy_payload
        )

        if_timeofftype_equals_KOR_annualleave = rail.IfOperator(
            task_id='if_timeofftype_equals_KOR_annualleave',
            test="{{ dag_run.conf.timeofftypes == 'KOR_Annual Leave 연차휴가' }}",
            yes_task="create_policylist",
            no_task="if_timeofftype_equals_KOR_monthlyleave_and_serviceyrs_less_than_2",
        )

        create_policylist = rail.SetVariableOperator(
            task_id='create_policylist',
            append=False,
            name='policylist',
            value=[]
        )

        create_yearlyentitilement = rail.SetVariableOperator(
            task_id='create_yearlyentitilement',
            append=False,
            name='yearlyentitilement',
            value=15
        )

        get_default_time_off_type_policy_schedule_for_user_for_annualleave = rail.RepliconServiceOperator(
            task_id="get_default_time_off_type_policy_schedule_for_user_for_annualleave",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeofftype_policy_sched_payload
        )

        log_yearly_accrual_24 = rail.PythonOperator(
            task_id='log_yearly_accrual_24',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_for_annualleave')[0]['policySet'],
                'script.name', "Yearly Accrual", "timeOffBalanceEventScripts.additionalParameters")).replace("[[", "[").replace("]]", "]")
        )

        log_existing_accrual_balance_26 = rail.PythonOperator(
            task_id='log_existing_accrual_balance_26',
            python_callable=lambda:  float(rail.find_first_by_attr_and_get_attr(rail.result(
                'log_yearly_accrual_24'), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number', ''))
        )

        log_existingaccrual_27 = rail.PythonOperator(
            task_id='log_existingaccrual_27',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('log_existing_accrual_balance_26')
                }
            }, ensure_ascii=False)
        )

        if_calendar_yos_less_than_2 = rail.IfOperator(
            task_id='if_calendar_yos_less_than_2',
            test=lambda: bool(rail.result('get_calendar_years_of_service') < 2),
            yes_task="log_numberofdaysforproration_for_yearly",
            no_task="if_calendar_yos_more_than_1_99",
        )

        log_numberofdaysforproration_for_yearly = rail.PythonOperator(
            task_id='log_numberofdaysforproration_for_yearly',
            python_callable=python_callable.get_number_of_days_proration
        )

        update_yearlyentitilement_30 = rail.SetVariableOperator(
            task_id='update_yearlyentitilement_30',
            append=False,
            name='{{ result("create_yearlyentitilement").name }}',
            value=python_callable.update_yearlyentitilement_val_30
        )

        if_calendar_yos_more_than_1_99 = rail.IfOperator(
            task_id='if_calendar_yos_more_than_1_99',
            test=lambda: bool(rail.result('get_calendar_years_of_service') > 1.99),
            yes_task="update_yearlyentitilement_32",
            no_task="log_accruals_rounded_value_34",
        )

        update_yearlyentitilement_32 = rail.SetVariableOperator(
            task_id='update_yearlyentitilement_32',
            append=False,
            name='{{ result("create_yearlyentitilement").name }}',
            value=lambda: round(((int(rail.result('get_calendar_years_of_service')) - 2) + float(rail.get_dag_run_var(
                rail.result('create_yearlyentitilement')['name']))), 2)
        )

        log_accruals_rounded_value_34 = rail.PythonOperator(
            task_id='log_accruals_rounded_value_34',
            python_callable=python_callable.accurals_rounded_val
        )

        log_newaccrual_35 = rail.PythonOperator(
            task_id='log_newaccrual_35',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('log_accruals_rounded_value_34')
                }
            }, ensure_ascii=False)
        )

        log_new_policy_to_assign_36 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_36',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_for_annualleave')[0]['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"').replace(
                            rail.result('log_existingaccrual_27'), rail.result('log_newaccrual_35')).replace(
                                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace(']}]', ']}'))
        )

        add_to_policy_list_38 = rail.SetVariableOperator(
            task_id='add_to_policy_list_38',
            append=True,
            name='{{ result("create_policylist").name }}',
            value=request_payload.add_to_policy_38
        )

        get_policylist_variable = rail.GetVariableOperator(
            task_id='get_policylist_variable',
            name='policylist'
        )

        log_policy_31 = rail.PythonOperator(
            task_id='log_policy_31',
            python_callable=lambda: json.loads(json.dumps(
                rail.result('get_policylist_variable')['value'], ensure_ascii=False).replace(
                '"policySet":[', '"policySet":').replace("}}]}]", "}}]}").replace(
                    'timeOffValidationScripts":[]}]}]', 'timeOffValidationScripts":[]}}]')) if rail.result(
                        'get_policylist_variable')['value'] else ''
        )

        put_user_timeoff_account_policysetschedule_40 = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policysetschedule_40',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_policy_31')
            }
        )

        if_timeofftype_equals_KOR_monthlyleave_and_serviceyrs_less_than_2 = rail.IfOperator(
            task_id='if_timeofftype_equals_KOR_monthlyleave_and_serviceyrs_less_than_2',
            test="{{ dag_run.conf.timeofftypes == 'KOR_Monthly Leave 월차휴가' and \
                result('get_years_of_service') < 2 }}",
            yes_task="get_all_scripts_timeOff_validation_script",
            no_task="final_response_from_dag",
        )

        get_all_scripts_timeOff_validation_script = rail.RepliconServiceOperator(
            task_id='get_all_scripts_timeOff_validation_script',
            endpoint='/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: {
                'prevent_bal': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Prevent balance overdraw', 'uri', '')
            }
        )

        get_timeoffbalance_event_script_administration_service = rail.RepliconServiceOperator(
            task_id='get_timeoffbalance_event_script_administration_service',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: {
                'startring_balance': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Starting Balance Set To', 'uri', '')
            }
        )

        create_policylist_monthlyleave = rail.SetVariableOperator(
            task_id='create_policylist_monthlyleave',
            append=False,
            name='policylist_monthlyleave',
            value=[]
        )

        create_monthlyentitlment = rail.SetVariableOperator(
            task_id='create_monthlyentitlment',
            append=False,
            name='monthlyentitlment',
            value=12
        )

        get_default_time_off_type_policy_schedule_for_user_48 = rail.RepliconServiceOperator(
            task_id="get_default_time_off_type_policy_schedule_for_user_48",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeofftype_policy_sched_payload
        )

        log_yearly_accrual_50 = rail.PythonOperator(
            task_id='log_yearly_accrual_50',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_48')[0]['policySet'],
                'script.name', "Yearly Accrual", "timeOffBalanceEventScripts")).replace('"script"', '"scriptTarget"').replace(
                    '[{"additionalParameters"', '{"additionalParameters"').replace('}}]', '}}').replace('do-not-prorate"}}', 'do-not-prorate"}}]')
        )

        log_yearly_reset_51 = rail.PythonOperator(
            task_id='log_yearly_reset_51',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_48')[0]['policySet'],
                'script.name', "Yearly Accrual", "timeOffBalanceEventScripts")).replace('"script"', '"scriptTarget"').replace(
                    '[{"additionalParameters"', '{"additionalParameters"').replace('}}]', '}}').replace('month:january"}}', 'month:january"}}]')
        )

        log_numberofdaysforproration_52 = rail.PythonOperator(
            task_id='log_numberofdaysforproration_52',
            python_callable=python_callable.get_number_of_days_proration
        )

        log_starting_balance_set_to_53 = rail.PythonOperator(
            task_id='log_starting_balance_set_to_53',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_48')[0]['policySet'],
                'script.name', "Starting Balance Set To", "timeOffBalanceEventScripts")).replace('}},"script"', '}}],"script"').replace(
                    '[{"additionalParameters"', '{"additionalParameters"').replace("}}]", "}}")
        )

        log_starting_bal_value_55 = rail.PythonOperator(
            task_id='log_starting_bal_value_55',
            python_callable=lambda:  float(rail.find_first_by_attr_and_get_attr(rail.result(
                'log_starting_balance_set_to_53'), 'keyUri', 'urn:replicon:script-key:parameter:amount', 'value.number', ''))
        )

        log_existing_start_bal_56 = rail.PythonOperator(
            task_id='log_existing_start_bal_56',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                    "number": rail.result('log_starting_bal_value_55')
                }
            }, ensure_ascii=False)
        )

        log_yearly_accrual_57 = rail.PythonOperator(
            task_id='log_yearly_accrual_57',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_48')[0]['policySet'],
                'script.name', "Yearly Accrual", "timeOffBalanceEventScripts.additionalParameters")).replace("[[", "[").replace("]]", "]")
        )

        log_existing_accrual_balance_59 = rail.PythonOperator(
            task_id='log_existing_accrual_balance_59',
            python_callable=lambda:  float(rail.find_first_by_attr_and_get_attr(rail.result(
                'log_yearly_accrual_57'), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number', ''))
        )

        log_existingaccrual_60 = rail.PythonOperator(
            task_id='log_existingaccrual_60',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('log_existing_accrual_balance_59')
                }
            }, ensure_ascii=False)
        )

        log_newaccrual_61 = rail.PythonOperator(
            task_id='log_newaccrual_61',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.get_dag_run_var(rail.result('create_monthlyentitlment')['name'])
                }
            }, ensure_ascii=False)
        )

        log_new_policy_to_assign_62 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_62',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_48')[0]['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"').replace(
                            rail.result('log_existingaccrual_60'), rail.result('log_newaccrual_61')).replace(
                                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace(
                                'timeOffValidationScripts":[]}]}', 'timeOffValidationScripts":[]}}').replace(']}]', ']}'))
        )

        create_noaccrualpolicy = rail.SetVariableOperator(
            task_id='create_noaccrualpolicy',
            append=False,
            name='noaccrualpolicy',
            value=[]
        )

        update_monthlyentitilement_67 = rail.SetVariableOperator(
            task_id='update_monthlyentitilement_67',
            append=False,
            name='{{ result("create_monthlyentitlment").name }}',
            value=lambda dag_run: int(rail.get_dag_run_var(
                rail.result('create_monthlyentitlment')['name'])) - int(datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d").month)
        )

        if_startdate_less_than_beginning_of_presentyear = rail.IfOperator(
            task_id='if_startdate_less_than_beginning_of_presentyear',
            test=lambda dag_run: bool(datetime.strptime(
                dag_run.conf['startdate'], "%Y-%m-%d") < datetime.now().replace(month=1, day=1)),
            yes_task="add_to_policy_list_68",
            no_task="if_startdate_equals_current_beginningyr",
        )

        add_to_policy_list_68 = rail.SetVariableOperator(
            task_id='add_to_policy_list_68',
            append=True,
            name='{{ result("create_policylist_monthlyleave").name }}',
            value=request_payload.add_to_policy_68
        )

        add_to_noaccrualpolicy_69 = rail.SetVariableOperator(
            task_id='add_to_noaccrualpolicy_69',
            append=True,
            name='{{ result("create_noaccrualpolicy").name }}',
            value=request_payload.add_to_noaccrualpolicy_94
        )

        log_3rd_yr_policyset = rail.PythonOperator(
            task_id='log_3rd_yr_policyset',
            python_callable=lambda: json.loads(json.dumps(rail.get_dag_run_var(rail.result('create_noaccrualpolicy')['name'])).replace(
                '{"additionalParameters":', '[{"additionalParameters":[').replace(',"script"', '],"script"').replace(
                    '}},"timeOffValidationScripts"', '}}],"timeOffValidationScripts"').replace(
                        "}}}", "}}]}"))
        )

        add_to_policy_list_72 = rail.SetVariableOperator(
            task_id='add_to_policy_list_72',
            append=True,
            name='{{ result("create_policylist_monthlyleave").name }}',
            value=request_payload.add_to_policy_72
        )

        if_startdate_equals_current_beginningyr = rail.IfOperator(
            task_id='if_startdate_equals_current_beginningyr',
            test=lambda dag_run: bool(datetime.strptime(
                dag_run.conf['startdate'], "%Y-%m-%d") == datetime.now().replace(month=1, day=1)),
            yes_task="log_newaccrual_77",
            no_task="log_newaccrual_83",
        )

        log_newaccrual_77 = rail.PythonOperator(
            task_id='log_newaccrual_77',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.get_dag_run_var(rail.result('create_monthlyentitlment')['name'])
                }
            }, ensure_ascii=False)
        )

        log_newstartbal_0_78 = rail.PythonOperator(
            task_id='log_newstartbal_0_78',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                    "number": 0
                }
            }, ensure_ascii=False)
        )

        log_new_policy_to_assign_79 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_79',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_48')[0]['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"').replace(
                            rail.result('log_existingaccrual_60'), rail.result('log_newaccrual_77')).replace(
                            rail.result('log_existing_start_bal_56'), rail.result('log_newstartbal_0_78')).replace(
                                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace(
                                'timeOffValidationScripts":[]}]}', 'timeOffValidationScripts":[]}}').replace(']}]', ']}'))
        )

        add_to_policy_list_81 = rail.SetVariableOperator(
            task_id='add_to_policy_list_81',
            append=True,
            name='{{ result("create_policylist_monthlyleave").name }}',
            value=request_payload.add_to_policy_81
        )

        log_newaccrual_83 = rail.PythonOperator(
            task_id='log_newaccrual_83',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": 0
                }
            }, ensure_ascii=False)
        )

        log_newstartbal_84 = rail.PythonOperator(
            task_id='log_newstartbal_84',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                    "number": rail.get_dag_run_var(rail.result('create_monthlyentitlment')['name'])
                }
            }, ensure_ascii=False)
        )

        log_new_policy_to_assign_85 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_85',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_48')[0]['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"').replace(
                            rail.result('log_existingaccrual_60'), rail.result('log_newaccrual_83')).replace(
                            rail.result('log_existing_start_bal_56'), rail.result('log_newstartbal_84')).replace(
                                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace(
                                'timeOffValidationScripts":[]}]}', 'timeOffValidationScripts":[]}}').replace(']}]', ']}'))
        )

        add_to_policy_list_87 = rail.SetVariableOperator(
            task_id='add_to_policy_list_87',
            append=True,
            name='{{ result("create_policylist_monthlyleave").name }}',
            value=request_payload.add_to_policy_87
        )

        update_monthlyentitilement_88 = rail.SetVariableOperator(
            task_id='update_monthlyentitilement_88',
            append=False,
            name='{{ result("create_monthlyentitlment").name }}',
            value=lambda: int(11 - int(rail.get_dag_run_var(
                rail.result('create_monthlyentitlment')['name'])))
        )

        log_newaccrual_89 = rail.PythonOperator(
            task_id='log_newaccrual_89',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.get_dag_run_var(rail.result('create_monthlyentitlment')['name'])
                }
            }, ensure_ascii=False)
        )

        log_new_policy_to_assign_90 = rail.PythonOperator(
            task_id='log_new_policy_to_assign_90',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_48')[0]['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"').replace(
                            rail.result('log_existingaccrual_60'), rail.result('log_newaccrual_89')).replace(
                                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace(']}]', ']}'))
        )

        add_to_policy_list_92 = rail.SetVariableOperator(
            task_id='add_to_policy_list_92',
            append=True,
            name='{{ result("create_policylist_monthlyleave").name }}',
            value=request_payload.add_to_policy_92
        )

        log_newstartbal_93 = rail.PythonOperator(
            task_id='log_newstartbal_93',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                    "number": 0
                }
            }, ensure_ascii=False)
        )

        add_to_noaccrualpolicy_94 = rail.SetVariableOperator(
            task_id='add_to_noaccrualpolicy_94',
            append=True,
            name='{{ result("create_noaccrualpolicy").name }}',
            value=request_payload.add_to_noaccrualpolicy_94
        )

        log_3rd_yr_policyset_95 = rail.PythonOperator(
            task_id='log_3rd_yr_policyset_95',
            python_callable=lambda: json.loads(json.dumps(rail.get_dag_run_var(rail.result('create_noaccrualpolicy')['name'])).replace(
                '{"additionalParameters":', '[{"additionalParameters":[').replace(',"script"', '],"script"').replace(
                    '}},"timeOffValidationScripts"', '}}],"timeOffValidationScripts"').replace(
                        "}}}", "}}]}"))
        )

        add_to_policy_list_97 = rail.SetVariableOperator(
            task_id='add_to_policy_list_97',
            append=True,
            name='{{ result("create_policylist_monthlyleave").name }}',
            value=request_payload.add_to_policy_97
        )

        final_policy = rail.PythonOperator(
            task_id='final_policy',
            python_callable=lambda: json.loads(json.dumps(rail.get_dag_run_var(rail.result('create_policylist_monthlyleave')['name'])).replace(
                ',"script"', '],"script"').replace('null', '"effective"').replace(
                    'policySet":[{', 'policySet":{').replace(']}]},{"description', ']}},{"description').replace(
                        "}}}", "}}]}").replace('}}]}]}]', '}}]}}]').replace('[]},{', '[],'))
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('final_policy')
            }
        )

        # Leaf boundary (common convention): capture the error for the parent to gather.
        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Timeoff add new user rehire - Dag_Run Error - {{ get_error_message() }}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_alltimeoff_types

        get_alltimeoff_types >> get_years_of_service >> get_calendar_years_of_service >> if_timeoffuri_is_present

        if_timeoffuri_is_present >> rail.Label('Yes') >> if_timeofftype_not_KOR_monthlyleave_and_annualleave
        if_timeoffuri_is_present >> rail.Label('No') >> final_response_from_dag

        if_timeofftype_not_KOR_monthlyleave_and_annualleave >> rail.Label('Yes') >> get_default_time_off_type_policy_schedule_for_user >> is_policy_present
        if_timeofftype_not_KOR_monthlyleave_and_annualleave >> rail.Label('No') >> if_timeofftype_equals_KOR_annualleave

        is_policy_present >> rail.Label('Yes') >> put_user_timeoff_policy >> final_response_from_dag
        is_policy_present >> rail.Label('No') >> final_response_from_dag

        if_timeofftype_equals_KOR_annualleave >> rail.Label('Yes') >> create_policylist
        if_timeofftype_equals_KOR_annualleave >> rail.Label('No') >> if_timeofftype_equals_KOR_monthlyleave_and_serviceyrs_less_than_2

        create_policylist >> create_yearlyentitilement >> get_default_time_off_type_policy_schedule_for_user_for_annualleave >> log_yearly_accrual_24 >> \
            log_existing_accrual_balance_26 >> log_existingaccrual_27 >> if_calendar_yos_less_than_2

        if_calendar_yos_less_than_2 >> rail.Label('Yes') >> log_numberofdaysforproration_for_yearly >> update_yearlyentitilement_30 >> \
            if_calendar_yos_more_than_1_99
        if_calendar_yos_less_than_2 >> rail.Label('No') >> if_calendar_yos_more_than_1_99

        if_calendar_yos_more_than_1_99 >> rail.Label('Yes') >> update_yearlyentitilement_32 >> log_accruals_rounded_value_34
        if_calendar_yos_more_than_1_99 >> rail.Label('No') >> log_accruals_rounded_value_34

        log_accruals_rounded_value_34 >> log_newaccrual_35 >> log_new_policy_to_assign_36 >> add_to_policy_list_38 >> get_policylist_variable >> \
            log_policy_31 >> put_user_timeoff_account_policysetschedule_40 >> if_timeofftype_equals_KOR_monthlyleave_and_serviceyrs_less_than_2

        if_timeofftype_equals_KOR_monthlyleave_and_serviceyrs_less_than_2 >> rail.Label('Yes') >> get_all_scripts_timeOff_validation_script
        if_timeofftype_equals_KOR_monthlyleave_and_serviceyrs_less_than_2 >> rail.Label('No') >> final_response_from_dag

        get_all_scripts_timeOff_validation_script >> get_timeoffbalance_event_script_administration_service >> create_policylist_monthlyleave >> \
            create_monthlyentitlment >> get_default_time_off_type_policy_schedule_for_user_48 >> log_yearly_accrual_50 >> log_yearly_reset_51 >> \
                log_numberofdaysforproration_52 >> log_starting_balance_set_to_53 >> log_starting_bal_value_55 >> log_existing_start_bal_56 >> \
                    log_yearly_accrual_57 >> log_existing_accrual_balance_59 >> log_existingaccrual_60 >> log_newaccrual_61 >> log_new_policy_to_assign_62 >> \
                        create_noaccrualpolicy >> update_monthlyentitilement_67 >> if_startdate_less_than_beginning_of_presentyear

        if_startdate_less_than_beginning_of_presentyear >> rail.Label('Yes') >> add_to_policy_list_68
        if_startdate_less_than_beginning_of_presentyear >> rail.Label('No') >> if_startdate_equals_current_beginningyr

        add_to_policy_list_68 >> add_to_noaccrualpolicy_69 >> log_3rd_yr_policyset >> add_to_policy_list_72 >> final_policy

        if_startdate_equals_current_beginningyr >> rail.Label('Yes') >> log_newaccrual_77 >> log_newstartbal_0_78 >> log_new_policy_to_assign_79 >> \
            add_to_policy_list_81 >> final_policy
        if_startdate_equals_current_beginningyr >> rail.Label('No') >> log_newaccrual_83 >> log_newstartbal_84 >> log_new_policy_to_assign_85 >> \
            add_to_policy_list_87 >> update_monthlyentitilement_88 >> log_newaccrual_89 >> log_new_policy_to_assign_90 >> add_to_policy_list_92 >> \
                log_newstartbal_93 >> add_to_noaccrualpolicy_94 >> log_3rd_yr_policyset_95 >> add_to_policy_list_97 >> final_policy

        final_policy >> put_user_time_off_account_policy_set_schedule >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
