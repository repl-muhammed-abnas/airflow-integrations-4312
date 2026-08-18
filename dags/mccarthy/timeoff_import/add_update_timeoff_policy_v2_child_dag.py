
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_timeoff_import_mccarthy_add_update_timeoff_policy_v2_{config.instance}',
        description=f'Mccarthy_Add/Update_Timeoff_Policy_V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_timeoff_import_child_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_timeoff_import_child_logs',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_timeoff_import_child_logs = rail.CreateLogOperator(
            task_id='create_timeoff_import_child_logs'
        )

        get_user_time_off_type_policy_summary_3 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_3',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_pluckuri_firstnil_blank_timeoffassignmentifnotassigned_4 = rail.IfOperator(
            task_id='if_pluckuri_firstnil_blank_timeoffassignmentifnotassigned_4',
            test=lambda dag_run: bool(rail.result('get_user_time_off_type_policy_summary_3') and rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_time_off_type_policy_summary_3')['policiesByTimeOffType'], 'timeOffType.name', dag_run.conf['timeofftype'], 'timeOffType.uri') is null),
            yes_task="time_off_import_logs_add_entry_5",
            no_task="declare_list_7",
        )

        time_off_import_logs_add_entry_5 = rail.WriteLogOperator(
            task_id='time_off_import_logs_add_entry_5',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="Timeoff {{ dag_run.conf.timeofftype }} not assigned for user {{ dag_run.conf.loginname }}",
            severity="Exception",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "status": "Exception",
                "details": "Timeoff {{ dag_run.conf.timeofftype }} not assigned for user {{ dag_run.conf.loginname }}",
                "jobid": "{{ dag_run.conf.jobid }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        declare_list_7 = rail.SetVariableOperator(
            task_id='declare_list_7',
            append=False,
            name='policysettoassign',
            value=[]
        )

        log_policy_schedules_based_on_timeoffuri = rail.PythonOperator(
            task_id='log_policy_schedules_based_on_timeoffuri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_type_policy_summary_3')['policiesByTimeOffType'],
                                                                                 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule')
        )

        if_effectivedate_day_present_10 = rail.IfOperator(
            task_id='if_effectivedate_day_present_10',
            test=lambda: bool(rail.result('log_policy_schedules_based_on_timeoffuri') and rail.result(
                'log_policy_schedules_based_on_timeoffuri')[0]['effectiveDate']['day']),
            yes_task="if_foreach_d_8_policysetschedule_greater_than_52_11",
            no_task="get_default_time_off_policy_set_schedule_for_time_off_type_19",
        )

        if_foreach_d_8_policysetschedule_greater_than_52_11 = rail.IfOperator(
            task_id='if_foreach_d_8_policysetschedule_greater_than_52_11',
            test='''{{ result('log_policy_schedules_based_on_timeoffuri') | length > 52 }}''',
            yes_task="invoke_custom_ruby_code_12",
            no_task="invoke_custom_ruby_code_16",
        )

        invoke_custom_ruby_code_12 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_12',
            python_callable=lambda dag_run: list(filter(lambda x: x['consider'] == "Yes" and x['consider2'] == "Yes", map(lambda item: {
                "description": item['description'],
                "effectiveDate": {
                    "day": item['effectiveDate']['day'],
                    "month": item['effectiveDate']['month'],
                    "year": item['effectiveDate']['year']
                },
                "policySet": item['policySet'],
                "consider": "Yes" if datetime.strptime((str(item['effectiveDate']['day']) + "/" + str(item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['year'])), "%d/%m/%Y") < datetime.strptime(dag_run.conf['effectivedate'], "%m/%d/%Y") else "No",
                "consider2": "Yes" if datetime.strptime((str(item['effectiveDate']['day']) + "/" + str(item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['year'])), "%d/%m/%Y") > ((datetime.today() - timedelta(days=365/12 * 13)).replace(day=1)) else "No",
            }, rail.result('log_policy_schedules_based_on_timeoffuri'))))

        )

        invoke_custom_ruby_code_16 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_16',
            python_callable=lambda dag_run: list(filter(lambda x: x['consider'] == "Yes", map(lambda item: {
                "description": item['description'],
                "effectiveDate": {
                    "day": item['effectiveDate']['day'],
                    "month": item['effectiveDate']['month'],
                    "year": item['effectiveDate']['year']
                },
                "policySet": item['policySet'],
                "consider": "Yes" if datetime.strptime((str(item['effectiveDate']['day']) + "/" + str(item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['year'])), "%d/%m/%Y") < datetime.strptime(dag_run.conf['effectivedate'], "%m/%d/%Y") else "No"
            }, rail.result('log_policy_schedules_based_on_timeoffuri'))))
        )

        get_default_time_off_policy_set_schedule_for_time_off_type_19 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type_19',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        declare_variable_20 = rail.SetVariableOperator(
            task_id='declare_variable_20',
            append=False,
            name='None',
            value=None
        )

        if_parameters_accruetype_present_21 = rail.IfOperator(
            task_id='if_parameters_accruetype_present_21',
            test=lambda dag_run: bool(dag_run.conf['accruetype']),
            yes_task="log_gettheaccrualbalancesetup_22",
            no_task="log_getthe_accrualscript_32",
        )

        log_gettheaccrualbalancesetup_22 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_22',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet']['timeOffBalanceEventScripts'],
                                                                         'script.name', 'Weekly Accrual')
        )

        log_gettheaccrualbalance_24 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_24',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(rail.result('log_gettheaccrualbalancesetup_22')['additionalParameters'],
                                                                               'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')) if rail.result('log_gettheaccrualbalancesetup_22') else ''
        )

        log_existing_accrual_25 = rail.PythonOperator(
            task_id='log_existing_accrual_25',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                                "value": {"number": rail.result("log_gettheaccrualbalance_24")}})
        )

        log_new_accrual_26 = rail.PythonOperator(
            task_id='log_new_accrual_26',
            python_callable=lambda dag_run: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": dag_run.conf["yearlyentitlement"]}}) if dag_run.conf["yearlyentitlement"] else '',
        )

        log_existing_week_27 = rail.PythonOperator(
            task_id='log_existing_week_27',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-week",
                                                "value": {"uri": "urn:replicon:day-of-week:monday"}}),
        )

        log_new_week_28 = rail.PythonOperator(
            task_id='log_new_week_28',
            python_callable=lambda dag_run: json.dumps({"keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-week", "value": {
                "uri": "urn:replicon:day-of-week:" + (dag_run.conf['onweek'] if dag_run.conf["onweek"] else 'Sunday')}})
        )

        update_variables_29 = rail.EmptyOperator(
            task_id='update_variables_29',
        )

        log_policy_set_30 = rail.PythonOperator(
            task_id='log_policy_set_30',
            python_callable=lambda: json.loads(json.dumps(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet'], ensure_ascii=False).replace(
                rail.result('log_existing_accrual_25'), rail.result('log_new_accrual_26')).replace(
                rail.result('log_existing_week_27'), rail.result('log_new_week_28')))
        )

        log_getthe_accrualscript_32 = rail.PythonOperator(
            task_id='log_getthe_accrualscript_32',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet']['timeOffBalanceEventScripts'],
                                                                                    'script.name', 'Weekly Accrual'), ensure_ascii=False)
        )

        update_policy_set_33 = rail.EmptyOperator(
            task_id='update_policy_set_33'
        )

        log_policy_set_34 = rail.PythonOperator(
            task_id='log_policy_set_34',
            python_callable=lambda: json.loads(json.dumps(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet'], ensure_ascii=False).replace(
                rail.result('log_getthe_accrualscript_32'), '""'))
        )

        if_parameters_allowedhours_present_35 = rail.IfOperator(
            task_id='if_parameters_allowedhours_present_35',
            test=lambda dag_run: bool(dag_run.conf['allowedhours']),
            yes_task="log_getthestartingbalancesetup_36",
            no_task="log_getthestartingbalancescript_44",
        )

        log_getthestartingbalancesetup_36 = rail.PythonOperator(
            task_id='log_getthestartingbalancesetup_36',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet']['timeOffBalanceEventScripts'],
                                                                         'script.name', 'Starting Balance Set To')
        )

        log_getthestartingbalance_38 = rail.PythonOperator(
            task_id='log_getthestartingbalance_38',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(rail.result('log_getthestartingbalancesetup_36')['additionalParameters'],
                                                                               'keyUri', 'urn:replicon:script-key:parameter:amount', 'value.number'))
        )

        log_existing_starting_balance_39 = rail.PythonOperator(
            task_id='log_existing_starting_balance_39',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                "value": {"number": rail.result('log_getthestartingbalance_38')}})
        )

        log_new_starting_balance_40 = rail.PythonOperator(
            task_id='log_new_starting_balance_40',
            python_callable=lambda dag_run: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": dag_run.conf['allowedhours']}})
        )

        update_variables_41 = rail.EmptyOperator(
            task_id='update_variables_41',
        )

        log_policy_set_42 = rail.PythonOperator(
            task_id='log_policy_set_42',
            python_callable=lambda: json.loads(json.dumps(rail.result('log_policy_set_30'), ensure_ascii=False).replace(
                rail.result('log_existing_starting_balance_39'), rail.result('log_new_starting_balance_40'))) if rail.result('log_policy_set_30') else json.loads(json.dumps(rail.result('log_policy_set_34')).replace(
                    rail.result('log_existing_starting_balance_39'), rail.result('log_new_starting_balance_40'))) if rail.result('log_policy_set_34') else json.loads(json.dumps(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet']).replace(
                        rail.result('log_existing_starting_balance_39'), rail.result('log_new_starting_balance_40'))) if rail.result('log_policy_set_30') else
            json.loads(json.dumps(rail.result('log_policy_set_34'), ensure_ascii=False).replace(rail.result('log_existing_starting_balance_39'), rail.result('log_new_starting_balance_40'))) if rail.result('log_policy_set_30') else json.loads(json.dumps(rail.result('log_policy_set_34')).replace(
                rail.result('log_existing_starting_balance_39'), rail.result('log_new_starting_balance_40'))) if rail.result('log_policy_set_34') else json.loads(json.dumps(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet']).replace(
                    rail.result('log_existing_starting_balance_39'), rail.result('log_new_starting_balance_40')))
        )

        log_getthestartingbalancescript_44 = rail.PythonOperator(
            task_id='log_getthestartingbalancescript_44',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet']['timeOffBalanceEventScripts'],
                                                                                    'script.name', 'Starting Balance Set To'))
        )

        update_variables_45 = rail.EmptyOperator(
            task_id='update_variables_45',
        )

        log_policy_set_46 = rail.PythonOperator(
            task_id='log_policy_set_46',
            python_callable=lambda: json.loads(json.dumps(rail.result('log_policy_set_30'), ensure_ascii=False).replace(
                rail.result('log_getthestartingbalancescript_44'), '""')) if rail.result('log_policy_set_30') else json.loads(json.dumps(rail.result('log_policy_set_34'), ensure_ascii=False).replace(
                    rail.result('log_getthestartingbalancescript_44'), '""')) if rail.result('log_policy_set_34') else json.loads(json.dumps(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19'), ensure_ascii=False).replace(
                        rail.result('log_getthestartingbalancescript_44'), '""'))
        )

        if_parameters_resettype_present_47 = rail.IfOperator(
            task_id='if_parameters_resettype_present_47',
            test=lambda dag_run: bool(dag_run.conf['resettype']),
            yes_task="log_gettheresetbalancesetup_48",
            no_task="log_gettheresetbalancescript_62",
        )

        log_gettheresetbalancesetup_48 = rail.PythonOperator(
            task_id='log_gettheresetbalancesetup_48',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet']['timeOffBalanceEventScripts'],
                                                                         'script.name', 'Yearly Reset')
        )

        log_getthe_resetbalance_50 = rail.PythonOperator(
            task_id='log_getthe_resetbalance_50',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(rail.result('log_gettheresetbalancesetup_48')['additionalParameters'],
                                                                               'keyUri', 'urn:replicon:script-key:parameter:reset-balance-amount', 'value.number')) if rail.result('log_gettheresetbalancesetup_48') else ''
        )

        log_existing_reset_balance_51 = rail.PythonOperator(
            task_id='log_existing_reset_balance_51',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                                                "value": {"number": rail.result('log_getthe_resetbalance_50')}})
        )

        log_new_reset_balance_52 = rail.PythonOperator(
            task_id='log_new_reset_balance_52',
            python_callable=lambda dag_run: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {"number": dag_run.conf['resetamount']}}) if dag_run.conf['resetamount'] else ''
        )

        log_existing_reset_month_53 = rail.PythonOperator(
            task_id='log_existing_reset_month_53',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:reset-on-month", "value": {"uri": "urn:replicon:month:january"}})
        )

        log_new_reset_month_54 = rail.PythonOperator(
            task_id='log_new_reset_month_54',
            python_callable=lambda dag_run: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-on-month", "value": {
                "uri": "urn:replicon:month:" + (dag_run.conf['resetonmonth'] if dag_run.conf['resetonmonth'] else 'january')}})
        )

        log_existing_reset_day_55 = rail.PythonOperator(
            task_id='log_existing_reset_day_55',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                                                "value": {"uri": "urn:replicon:monthly-frequency-start-day-option:1st"}})
        )

        log_new_reset_day_56 = rail.PythonOperator(
            task_id='log_new_reset_day_56',
            python_callable=lambda dag_run: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month", "value": {
                "uri": "urn:replicon:monthly-frequency-start-day-option:" + (dag_run.conf['ondayofmonth'] if dag_run.conf['ondayofmonth'] else '1st')}})
        )

        log_get_present_reset_type_57 = rail.PythonOperator(
            task_id='log_get_present_reset_type_57',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('log_gettheresetbalancesetup_48')['additionalParameters'],
                                                                         'keyUri', 'urn:replicon:script-key:parameter:periodic-reset-option', 'value.uri') if rail.result('log_gettheresetbalancesetup_48') else ''
        )

        log_existing_reset_type_57 = rail.PythonOperator(
            task_id='log_existing_reset_type_57',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:periodic-reset-option",
                                                "value": {"uri": rail.result('log_get_present_reset_type_57')}})
        )

        log_new_reset_type_58 = rail.PythonOperator(
            task_id='log_new_reset_type_58',
            python_callable=lambda dag_run: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:periodic-reset-option", "value": {"uri": dag_run.conf['resettypeuri']}})
        )

        update_variables_59 = rail.EmptyOperator(
            task_id='update_variables_59',
        )

        log_policy_set_60 = rail.PythonOperator(
            task_id='log_policy_set_60',
            python_callable=lambda: json.loads(json.dumps(rail.result('log_policy_set_46'), ensure_ascii=False).replace(
                rail.result('log_existing_reset_balance_51'), rail.result('log_new_reset_balance_52')).replace(
                rail.result('log_existing_reset_month_53'), rail.result('log_new_reset_month_54')).replace(
                rail.result('log_existing_reset_day_55'), rail.result('log_new_reset_day_56')).replace(
                rail.result('log_existing_reset_type_57'), rail.result('log_new_reset_type_58'))) if rail.result('log_policy_set_46') else
            json.loads(json.dumps(rail.result('log_policy_set_42'), ensure_ascii=False).replace(
                rail.result('log_existing_reset_balance_51'), rail.result('log_new_reset_balance_52')).replace(
                rail.result('log_existing_reset_month_53'), rail.result('log_new_reset_month_54')).replace(
                rail.result('log_existing_reset_day_55'), rail.result('log_new_reset_day_56')).replace(
                rail.result('log_existing_reset_type_57'), rail.result('log_new_reset_type_58'))) if rail.result('log_policy_set_42') else
            json.loads(json.dumps(rail.result('log_policy_set_30'), ensure_ascii=False).replace(
                rail.result('log_existing_reset_balance_51'), rail.result('log_new_reset_balance_52')).replace(
                rail.result('log_existing_reset_month_53'), rail.result('log_new_reset_month_54')).replace(
                rail.result('log_existing_reset_day_55'), rail.result('log_new_reset_day_56')).replace(
                rail.result('log_existing_reset_type_57'), rail.result('log_new_reset_type_58'))) if rail.result('log_policy_set_30') else
            json.loads(json.dumps(rail.result('log_policy_set_34'), ensure_ascii=False).replace(
                rail.result('log_existing_reset_balance_51'), rail.result('log_new_reset_balance_52')).replace(
                rail.result('log_existing_reset_month_53'), rail.result('log_new_reset_month_54')).replace(
                rail.result('log_existing_reset_day_55'), rail.result('log_new_reset_day_56')).replace(
                rail.result('log_existing_reset_type_57'), rail.result('log_new_reset_type_58'))) if rail.result('log_policy_set_34') else
            json.loads(json.dumps(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet'], ensure_ascii=False).replace(
                rail.result('log_existing_reset_balance_51'), rail.result('log_new_reset_balance_52')).replace(
                rail.result('log_existing_reset_month_53'), rail.result('log_new_reset_month_54')).replace(
                rail.result('log_existing_reset_day_55'), rail.result('log_new_reset_day_56')).replace(
                rail.result('log_existing_reset_type_57'), rail.result('log_new_reset_type_58')))
        )

        log_gettheresetbalancescript_62 = rail.PythonOperator(
            task_id='log_gettheresetbalancescript_62',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet']['timeOffBalanceEventScripts'],
                                                                                    'script.name', 'Yearly Reset'), ensure_ascii=False)
        )

        log_policy_set_64 = rail.PythonOperator(
            task_id='log_policy_set_64',
            python_callable=lambda: (json.loads(json.dumps(rail.result('log_policy_set_46'), ensure_ascii=False).replace(
                rail.result('log_gettheresetbalancescript_62'), '""')) if rail.result('log_policy_set_46') else
                json.loads(json.dumps(rail.result('log_policy_set_42'), ensure_ascii=False).replace(
                    rail.result('log_gettheresetbalancescript_62'), '""')) if rail.result('log_policy_set_42') else
                json.loads(json.dumps(rail.result('log_policy_set_30'), ensure_ascii=False).replace(
                    rail.result('log_gettheresetbalancescript_62'), '""')) if rail.result('log_policy_set_30') else
                json.loads(json.dumps(rail.result('log_policy_set_34'), ensure_ascii=False).replace(
                    rail.result('log_gettheresetbalancescript_62'), '""')) if rail.result('log_policy_set_34') else
                json.loads(json.dumps(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_19')[0]['policySet'], ensure_ascii=False).replace(
                    rail.result('log_gettheresetbalancescript_62'), '""')))
        )

        log_policysettoprocess_65 = rail.PythonOperator(
            task_id="log_policysettoprocess_65",
            python_callable=lambda: (json.loads(json.dumps(rail.result('log_policy_set_60'), ensure_ascii=False).replace("[,{", "[{")
                                                .replace("},,{", "},{").replace('"timeOffValidationScripts":}', '"timeOffValidationScripts":[]}')
                                                .replace('"timeOffValidationScripts": ""}', '"timeOffValidationScripts":[]}')
                                                .replace("[]}]", "[]}").replace("}},]", "}}]").replace("[,{", "[{")
                                                .replace('}},],"timeOffValidationScripts', '}}],"timeOffValidationScripts')
                                                .replace('"timeOffValidationScripts":}', '"timeOffValidationScripts":[]}').replace("[]}]", "[]}")
                                                .replace("}}]}]", "}}]}").replace('"additionalParameters":,', '"additionalParameters":[],')
                                                .replace(",,", "").replace('["",', '[')
                                                .replace('"additionalParameters": "",', '"additionalParameters": [],')
                                                .replace('}, "", ""]', '}]').replace('"",', '')
                                                .replace('}, ""', '}'))) if rail.result('log_policy_set_60') else
                                    ((json.loads(json.dumps(rail.result('log_policy_set_64'), ensure_ascii=False).replace("[,{", "[{")
                                                 .replace("},,{", "},{").replace('"timeOffValidationScripts":}', '"timeOffValidationScripts":[]}')
                                                 .replace('"timeOffValidationScripts": ""}', '"timeOffValidationScripts":[]}')
                                                 .replace("[]}]", "[]}").replace("}},]", "}}]").replace("[,{", "[{")
                                                 .replace('}},],"timeOffValidationScripts', '}}],"timeOffValidationScripts')
                                                 .replace('"timeOffValidationScripts":}', '"timeOffValidationScripts":[]}').replace("[]}]", "[]}")
                                                 .replace("}}]}]", "}}]}").replace('"additionalParameters":,', '"additionalParameters":[],')
                                                 .replace(",,", "").replace('["",', '[')
                                                 .replace('"additionalParameters": "",', '"additionalParameters": [],')
                                                 .replace('}, "", ""]', '}]').replace('"",', '')
                                                 .replace('}, ""', '}'))))
        )

        parse_json_66 = rail.EmptyOperator(
            task_id='parse_json_66'
        )

        def get_policy_schedule_list(dag_run):

            effective_date = datetime.strptime(
                dag_run.conf['effectivedate'], "%m/%d/%Y").strftime("%d/%m/%Y")

            def compare_dates(existing_policy_date):
                existing_date = datetime(
                    existing_policy_date['year'], existing_policy_date['month'], existing_policy_date['day'])
                new_date = datetime.strptime(
                    dag_run.conf['effectivedate'], "%m/%d/%Y")
                return existing_date != new_date

            policy_schedule_list = []
            policy_schedules = []
            if rail.result("invoke_custom_ruby_code_12"):
                policy_schedules.extend(
                    rail.result('invoke_custom_ruby_code_12'))
            if rail.result("invoke_custom_ruby_code_16"):
                policy_schedules.extend(
                    rail.result('invoke_custom_ruby_code_16'))

            policy_schedule_list = [
                policy_schedule for policy_schedule in policy_schedules if compare_dates(policy_schedule['effectiveDate'])]
            policy_schedule_list.append({
                "description": "Effective On " + effective_date,
                "effectiveDate": {
                    "day": int(effective_date.split('/')[0]),
                    "month": int(effective_date.split('/')[1]),
                    "year": int(effective_date.split('/')[2])
                },
                "policySet": rail.result("log_policysettoprocess_65")
            })
            return policy_schedule_list

        insert_to_list_67 = rail.PythonOperator(
            task_id="insert_to_list_67",
            python_callable=get_policy_schedule_list
        )

        def get_policy_sets():
            policy_sets = rail.result('insert_to_list_67')
            if policy_sets and policy_sets[0]['effectiveDate']['day']:
                return json.loads(json.dumps(policy_sets, ensure_ascii=False).replace(
                    '"script"', '"scriptTarget"'))
            return ''
        log_policy_to_assign_68 = rail.PythonOperator(
            task_id='log_policy_to_assign_68',
            python_callable=get_policy_sets
        )

        put_user_time_off_account_policy_set_schedule_69 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_69',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_policy_to_assign_68')
            }
        )

        time_off_import_logs_add_entry_70 = rail.WriteLogOperator(
            task_id='time_off_import_logs_add_entry_70',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="Policy Added Successfully",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "status": "Success",
                "details": "Policy Added Successfully",
                "jobid": "{{ dag_run.conf.jobid }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "jobid": "{{ dag_run.conf.jobid }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> create_timeoff_import_child_logs >> get_user_time_off_type_policy_summary_3 >> if_pluckuri_firstnil_blank_timeoffassignmentifnotassigned_4
        if_pluckuri_firstnil_blank_timeoffassignmentifnotassigned_4 >> rail.Label(
            'Yes') >> time_off_import_logs_add_entry_5 >> catch_and_log_error
        if_pluckuri_firstnil_blank_timeoffassignmentifnotassigned_4 >> rail.Label(
            'No') >> declare_list_7 >> log_policy_schedules_based_on_timeoffuri >> if_effectivedate_day_present_10
        if_effectivedate_day_present_10 >> rail.Label(
            'Yes') >> if_foreach_d_8_policysetschedule_greater_than_52_11
        if_foreach_d_8_policysetschedule_greater_than_52_11 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_12 >> get_default_time_off_policy_set_schedule_for_time_off_type_19
        if_foreach_d_8_policysetschedule_greater_than_52_11 >> rail.Label(
            'No') >> invoke_custom_ruby_code_16 >> get_default_time_off_policy_set_schedule_for_time_off_type_19
        if_effectivedate_day_present_10 >> rail.Label(
            'No') >> get_default_time_off_policy_set_schedule_for_time_off_type_19 >> declare_variable_20 \
            >> if_parameters_accruetype_present_21
        if_parameters_accruetype_present_21 >> rail.Label(
            'Yes') >> log_gettheaccrualbalancesetup_22 >> log_gettheaccrualbalance_24 \
            >> log_existing_accrual_25 >> log_new_accrual_26 >> log_existing_week_27 >> log_new_week_28 \
            >> update_variables_29 >> log_policy_set_30 >> if_parameters_allowedhours_present_35
        if_parameters_accruetype_present_21 >> rail.Label(
            'No') >> log_getthe_accrualscript_32 >> update_policy_set_33 >> log_policy_set_34 >> if_parameters_allowedhours_present_35
        if_parameters_allowedhours_present_35 >> rail.Label(
            'Yes') >> log_getthestartingbalancesetup_36 >> log_getthestartingbalance_38 \
            >> log_existing_starting_balance_39 >> log_new_starting_balance_40 >> update_variables_41 >> log_policy_set_42 >> if_parameters_resettype_present_47
        if_parameters_allowedhours_present_35 >> rail.Label(
            'No') >> log_getthestartingbalancescript_44 >> update_variables_45 >> log_policy_set_46 >> if_parameters_resettype_present_47
        if_parameters_resettype_present_47 >> rail.Label(
            'Yes') >> log_gettheresetbalancesetup_48 \
            >> log_getthe_resetbalance_50 >> log_existing_reset_balance_51 >> log_new_reset_balance_52 >> log_existing_reset_month_53 \
            >> log_new_reset_month_54 >> log_existing_reset_day_55 >> log_new_reset_day_56 >> log_get_present_reset_type_57 >> log_existing_reset_type_57 \
            >> log_new_reset_type_58 >> update_variables_59 >> log_policy_set_60 >> log_policysettoprocess_65
        if_parameters_resettype_present_47 >> rail.Label(
            'No') >> log_gettheresetbalancescript_62 >> log_policy_set_64 >> log_policysettoprocess_65
        log_policysettoprocess_65 >> parse_json_66 >> insert_to_list_67 >> log_policy_to_assign_68 \
            >> put_user_time_off_account_policy_set_schedule_69 >> time_off_import_logs_add_entry_70 \
            >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
