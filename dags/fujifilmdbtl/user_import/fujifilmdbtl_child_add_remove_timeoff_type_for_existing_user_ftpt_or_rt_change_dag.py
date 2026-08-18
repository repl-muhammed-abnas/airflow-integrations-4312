from datetime import timedelta, datetime
from pendulum import now
import json
from airflow.models import Variable
import rail
from fujifilmdbtl.user_import.mapper.fujifilmdbtl_timeoff_balance_mapper import fdt_timeoff_balance_mapper
from fujifilmdbtl.user_import.utils.python_callable import get_split_date, get_timeoff_type_uris

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_child_add_remove_timeoff_type_for_existing_user_ftpt_or_rt_change_{config.instance}',
        description=f'FDT_Child Workflow to add/remove timeoff type for existing user-FTPT or RT Change {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config',
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_tenureoftheuser_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_tenureoftheuser_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_tenureoftheuser_3 = rail.PythonOperator(
            task_id='log_tenureoftheuser_3',
            python_callable=lambda dag_run:  ((now().date() - datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y").date()).days) / 365
        )

        declare_list_4 = rail.SetVariableOperator(
            task_id='declare_list_4',
            append=False,
            name='assigned_timeoff_types',
            value=[]
        )

        invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5',
            python_callable=lambda: rail.get_replicon_date(now().add(months=1).start_of('month'))
        )

        get_user_time_off_type_policy_summary_6 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_6',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        foreach_d_7 = rail.ForEachOperator(
            task_id='foreach_d_7',
            items=lambda: rail.result(
                'get_user_time_off_type_policy_summary_6')['policiesByTimeOffType'],
            start_task='if_foreach_d_7_istimeoffallowedagainstthistimeofftype_is_true_8',
            end_task='insert_to_list_9'
        )

        if_foreach_d_7_istimeoffallowedagainstthistimeofftype_is_true_8 = rail.IfOperator(
            task_id='if_foreach_d_7_istimeoffallowedagainstthistimeofftype_is_true_8',
            test='''{{ result('foreach_d_7').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="insert_to_list_9",
            no_task="foreach_d_7_end",
        )

        insert_to_list_9 = rail.SetVariableOperator(
            task_id='insert_to_list_9',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "name": "{{ result('foreach_d_7').timeOffType.name }}",
                "uri": "{{ result('foreach_d_7').timeOffType.uri }}",
                "policyset": "{{result('foreach_d_7').policySetSchedule}}"
            }
        )

        foreach_d_7_end = rail.EmptyOperator(
            task_id='foreach_d_7_end',
        )

        _adhoc_http_action_10 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_10',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        if_first_displaytext_present_11 = rail.IfOperator(
            task_id='if_first_displaytext_present_11',
            test='''{{ result('_adhoc_http_action_10')[0].displayText' | is_truthy }}''',
            yes_task="fdt_timeoff_balance_mapper_search_entries_12",
            no_task="catch_errors",
        )

        fdt_timeoff_balance_mapper_search_entries_12 = rail.PythonOperator(
            task_id='fdt_timeoff_balance_mapper_search_entries_12',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "timeoff" and x["monthofhire"] == dag_run.conf['regulartemp'] and x["ftpt"] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper))
        )

        declare_list_13 = rail.SetVariableOperator(
            task_id='declare_list_13',
            append=False,
            name='timeoff_types_to_assign',
            value=[]
        )

        foreach_fdt_timeoff_balance_mapper_search_entries_12_14 = rail.ForEachOperator(
            task_id='foreach_fdt_timeoff_balance_mapper_search_entries_12_14',
            items=lambda: rail.result(
                'fdt_timeoff_balance_mapper_search_entries_12'),
            start_task='insert_to_list_15',
            end_task='accumulate_list_items_16'
        )

        insert_to_list_15 = rail.SetVariableOperator(
            task_id='insert_to_list_15',
            append=True,
            name='{{ result("declare_list_13").name }}',
            value=lambda: {
                "name": rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_12_14')['balance'],
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_10'), 'displayText', rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_12_14')['balance'], 'uri', "")
            }
        )

        accumulate_list_items_16 = rail.SetVariableOperator(
            task_id='accumulate_list_items_16',
            name='logging',
            append=True,
            value=lambda: {
                "name": rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_12_14')['balance'],
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_10'), 'displayText', rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_12_14')['balance'], 'uri', "")
            }
        )

        foreach_fdt_timeoff_balance_mapper_search_entries_12_14_end = rail.EmptyOperator(
            task_id='foreach_fdt_timeoff_balance_mapper_search_entries_12_14_end',
        )

        log_final_set_timeoff_uris_17 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_17',
            python_callable=lambda: get_timeoff_type_uris(rail.get_dag_run_var('timeoff_types_to_assign')) if rail.get_dag_run_var(
                'timeoff_types_to_assign') else ""
        )

        if_log_12_present_18 = rail.IfOperator(
            task_id='if_log_12_present_18',
            test='''{{ result('log_final_set_timeoff_uris_17') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_19",
            no_task="catch_errors",
        )

        put_time_off_type_assignments_for_user_19 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_19',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_17')
            }
        )

        get_user_time_off_type_policy_summary_20 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_20',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_declare_list_4_value = rail.GetVariableOperator(
            task_id='get_declare_list_4_value',
            name='assigned_timeoff_types'
        )

        # Initialize variable to collect DAG runs from foreach loop
        init_timeoff_dag_runs_list_21 = rail.SetVariableOperator(
            task_id='init_timeoff_dag_runs_list_21',
            name='timeoff_dag_runs_21',
            append=False,
            value=[]
        )

        foreach_declare_list_4_21 = rail.ForEachOperator(
            task_id='foreach_declare_list_4_21',
            items=lambda: rail.result('get_declare_list_4_value'),
            start_task='log_ifthetimeoff_typeisnotrequiredanymore_22',
            end_task='foreach_declare_list_4_21_end'
        )

        log_ifthetimeoff_typeisnotrequiredanymore_22 = rail.PythonOperator(
            task_id='log_ifthetimeoff_typeisnotrequiredanymore_22',
            python_callable=lambda: rail.result('foreach_declare_list_4_21')['uri'] if rail.find_first_by_attr_and_get_attr(
                rail.result('accumulate_list_items_16'), 'uri', rail.result('foreach_declare_list_4_21')['uri'], 'uri', "") else ""
        )

        if_log_ifthetimeoff_typeisnotrequiredanymore_22_blank_23 = rail.IfOperator(
            task_id='if_log_ifthetimeoff_typeisnotrequiredanymore_22_blank_23',
            test='''{{ result('log_ifthetimeoff_typeisnotrequiredanymore_22') | is_falsy }}''',
            yes_task="get_balance_summary_for_account_24",
            no_task="foreach_declare_list_4_21_end",
        )

        get_balance_summary_for_account_24 = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account_24',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data={
                "account": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_declare_list_4_21').uri }}"
                },
                "asOfDate": {
                    "year": "{{ result('invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5').day }}"
                }
            }
        )

        trigger_dag_run_timeoff_policy_update_on_each_time_off_type_for_no_accrual_025 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_timeoff_policy_update_on_each_time_off_type_for_no_accrual_025',
            retries=0,
            trigger_dag_id=f'fujifilmdbtl_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('foreach_declare_list_4_21')['uri'],
                "policyset": json.dumps(rail.result('foreach_declare_list_4_21')['policyset']),
                "enddate": now().strftime("%d/%m/%Y"),
                "newschedulebalance": rail.result('get_balance_summary_for_account_24')['timeRemaining']
            }
        )

        # Append the triggered DAG run to the collection variable
        append_timeoff_dag_run_025 = rail.SetVariableOperator(
            task_id='append_timeoff_dag_run_025',
            name='timeoff_dag_runs_21',
            append=True,
            value='{{ result("trigger_dag_run_timeoff_policy_update_on_each_time_off_type_for_no_accrual_025") }}'
        )

        foreach_declare_list_4_21_end = rail.EmptyOperator(
            task_id='foreach_declare_list_4_21_end',
        )

        # Process the collected DAG run IDs
        get_timeoff_child_dag_ids_21 = rail.PythonOperator(
            task_id='get_timeoff_child_dag_ids_21',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('timeoff_dag_runs_21')] if rail.get_dag_run_var('timeoff_dag_runs_21') else []
        )

        # Wait for all collected DAG runs after the foreach loop completes
        wait_for_all_timeoff_dag_runs_21 = rail.WaitForDagRunsSensor(
            task_id='wait_for_all_timeoff_dag_runs_21',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_timeoff_child_dag_ids_21') | to_json }}"
        )

        foreach_d_26 = rail.ForEachOperator(
            task_id='foreach_d_26',
            items=lambda: rail.result('get_user_time_off_type_policy_summary_20')[
                'policiesByTimeOffType'],
            start_task='if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_27',
            end_task='foreach_d_26_end'
        )

        if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_27 = rail.IfOperator(
            task_id='if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_27',
            test='''{{ result('foreach_d_26').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="declare_list_28",
            no_task="foreach_d_26_end",
        )

        declare_list_28 = rail.SetVariableOperator(
            task_id='declare_list_28',
            append=False,
            name='policytoassign',
            value=[]
        )

        log_checkifthetimeoffisalreadyassigned_29 = rail.PythonOperator(
            task_id='log_checkifthetimeoffisalreadyassigned_29',
            python_callable=lambda: "True" if rail.result('declare_list_4')[
                'uri'] == rail.result('foreach_d_26')['timeOffType']['uri'] else ""
        )

        get_default_time_off_policy_set_schedule_for_time_off_type_31 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type_31',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('foreach_d_26')['timeOffType']['uri']
            }
        )

        foreach_response_33 = rail.ForEachOperator(
            task_id='foreach_response_33',
            items=lambda: rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type_31'),
            start_task='if_foreach_response_33_indexforeach_meta_equals_to_0_34',
            end_task='foreach_response_33_end'
        )

        if_foreach_response_33_indexforeach_meta_equals_to_0_34 = rail.IfOperator(
            task_id='if_foreach_response_33_indexforeach_meta_equals_to_0_34',
            test=lambda: rail.result('foreach_response_33') == rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type_31')[0],
            yes_task="invoke_custom_ruby_code_todaysdate_35",
            no_task="invoke_custom_ruby_code_future_effective_date_38",
        )

        invoke_custom_ruby_code_todaysdate_35 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_todaysdate_35',
            python_callable=lambda: get_split_date(now())
        )

        insert_to_list_36 = rail.SetVariableOperator(
            task_id='insert_to_list_36',
            append=True,
            name='policytoassign',
            value=lambda: {
                "effectiveDate": {
                    "day": rail.render_template("{{ result('invoke_custom_ruby_code_todaysdate_35').day }}"),
                    "month": rail.render_template("{{ result('invoke_custom_ruby_code_todaysdate_35').month }}"),
                    "year": rail.render_template("{{ result('invoke_custom_ruby_code_todaysdate_35').year }}")
                },
                "description": rail.render_template("Effective on {{ result('invoke_custom_ruby_code_todaysdate_35').day }}/{{ result('invoke_custom_ruby_code_todaysdate_35').month }}/{{ result('invoke_custom_ruby_code_todaysdate_35').year }}"),
                "policySet": rail.result('foreach_response_33')['policySet']
            }
        )

        invoke_custom_ruby_code_future_effective_date_38 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_future_effective_date_38',
            python_callable=lambda: get_split_date(now().replace(
                year=now().year + rail.result('foreach_response_33')['startOffset']['offsetValue']))
        )

        insert_to_list_39 = rail.SetVariableOperator(
            task_id='insert_to_list_39',
            append=True,
            name='{{ result("declare_list_28").name }}',
            value=lambda: {
                "effectiveDate": {
                    "day": rail.render_template("{{ result('invoke_custom_ruby_code_future_effective_date_38').day }}"),
                    "month": rail.render_template("{{ result('invoke_custom_ruby_code_future_effective_date_38').month }}"),
                    "year": rail.render_template("{{ result('invoke_custom_ruby_code_future_effective_date_38').year }}")
                },
                "description": rail.render_template("Effective on {{ result('invoke_custom_ruby_code_future_effective_date_38').day }}/{{ result('invoke_custom_ruby_code_future_effective_date_38').month }}/{{ result('invoke_custom_ruby_code_future_effective_date_38').year }}"),
                "policySet": rail.result('foreach_response_33')['policySet']
            }
        )

        foreach_response_33_end = rail.EmptyOperator(
            task_id='foreach_response_33_end',
        )

        log_timeoff_policy_40 = rail.PythonOperator(
            task_id='log_timeoff_policy_40',
            python_callable=lambda: json.dumps(rail.result('declare_list_28')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"') if rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_31')[0]['policySet'] else ""
        )

        if_log_13_present_41 = rail.IfOperator(
            task_id='if_log_13_present_41',
            test='''{{ result('log_timeoff_policy_40') | is_truthy }}''',
            yes_task="log_reset_balancefrompolicy_42",
            no_task="if_log_checkifthetimeoffisalreadyassigned_29_blank_50",
        )

        log_reset_balancefrompolicy_42 = rail.PythonOperator(
            task_id='log_reset_balancefrompolicy_42',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_31')[
                                               0]['policySet']['timeOffBalanceEventScripts'], 'script.name', "Yearly Reset", 'additionalParameters')).replace("[[", "[").replace("]]", "]")
        )

        parse_json_43 = rail.PythonOperator(
            task_id='parse_json_43',
            python_callable=lambda: json.loads(
                rail.result('log_reset_balancefrompolicy_42'))
        )

        log_reset_balance_44 = rail.PythonOperator(
            task_id='log_reset_balance_44',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_43'), 'keyUri', "urn:replicon:script-key:parameter:reset-balance-amount", 'value.number', ""))
        )

        log_defaultresetamountforsickleave_45 = rail.PythonOperator(
            task_id='log_defaultresetamountforsickleave_45',
            python_callable=lambda: {
                "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                "value": {
                    "number": rail.result('log_reset_balance_44')
                }
            }
        )

        declare_variable_46 = rail.SetVariableOperator(
            task_id='declare_variable_46',
            append=False,
            name='sick_reset_amount',
            value=None
        )

        if_name_downcase_equals_to_sickleave_47 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_47',
            test=lambda dag_run: rail.result('foreach_d_26')['timeOffType']['name'].lower(
            ) == 'sick leave' and dag_run.conf.ftpt == 'p',
            yes_task="log_reset_balanceforparttime_48",
            no_task="if_log_checkifthetimeoffisalreadyassigned_29_blank_50",
        )

        log_reset_balanceforparttime_48 = rail.PythonOperator(
            task_id='log_reset_balanceforparttime_48',
            python_callable=lambda: str(
                float(float(rail.result('log_reset_balance_44')) / 2))
        )

        update_variable_49 = rail.SetVariableOperator(
            task_id='update_variable_49',
            append=False,
            name='sick_reset_amount',
            value=lambda: {
                "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                "value": {
                    "number": rail.result('log_reset_balanceforparttime_48')
                }
            }
        )

        if_log_checkifthetimeoffisalreadyassigned_29_blank_50 = rail.IfOperator(
            task_id='if_log_checkifthetimeoffisalreadyassigned_29_blank_50',
            test='''{{ result('log_checkifthetimeoffisalreadyassigned_29') | is_falsy }}''',
            yes_task="accumulate_list_items_51",
            no_task="if_log_checkifthetimeoffisalreadyassigned_29_present_69",
        )

        accumulate_list_items_51 = rail.SetVariableOperator(
            task_id='accumulate_list_items_51',
            name='assigned_timeoff_types',
            append=True,
            value=lambda: {
                "timeofftype": rail.result('foreach_d_26')['timeOffType']['name']
            }
        )

        if_log_13_present_52 = rail.IfOperator(
            task_id='if_log_13_present_52',
            test='''{{ result('log_timeoff_policy_40') | is_truthy }}''',
            yes_task="if_name_downcase_equals_to_sickleave_53",
            no_task="if_log_checkifthetimeoffisalreadyassigned_29_present_69",
        )

        if_name_downcase_equals_to_sickleave_53 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_53',
            test=lambda: rail.result('foreach_d_26')['timeOffType']['name'].lower() == 'sick leave' or rail.result(
                'foreach_d_26')['timeOffType']['name'].lower() == 'floating holiday',
            yes_task="log_lookupbalancebasedonmonthofhire_54",
            no_task="put_user_time_off_account_policy_set_schedule_68",
        )

        log_lookupbalancebasedonmonthofhire_54 = rail.PythonOperator(
            task_id='log_lookupbalancebasedonmonthofhire_54',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == (rail.result('foreach_d_26')['timeOffType']['name']).lower() and x["monthofhire"] == dag_run.conf['startdatemonth'] and x['ftpt'] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper))[0]['balance']
        )

        if_log_lookupbalancebasedonmonthofhire_54_present_55 = rail.IfOperator(
            task_id='if_log_lookupbalancebasedonmonthofhire_54_present_55',
            test='''{{ result('log_lookupbalancebasedonmonthofhire_54') | is_truthy }}''',
            yes_task="log_initial_balancefrompolicy_56",
            no_task="log_timeoff_policy_65",
        )

        log_initial_balancefrompolicy_56 = rail.PythonOperator(
            task_id='log_initial_balancefrompolicy_56',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_31')[
                                               0]['policySet']['timeOffBalanceEventScripts'], 'script.name', "Starting Balance Set To", 'additionalParameters')).replace("[[", "[").replace("]]", "]")
        )

        parse_json_57 = rail.PythonOperator(
            task_id='parse_json_57',
            python_callable=lambda: json.loads(
                rail.result('log_initial_balancefrompolicy_56'))
        )

        log_initial_balance_58 = rail.PythonOperator(
            task_id='log_initial_balance_58',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_57'), 'keyUri', "urn:replicon:script-key:parameter:amount", 'value.number'))
        )

        log_valuefromdefault_59 = rail.PythonOperator(
            task_id='log_valuefromdefault_59',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                "value": {"number": rail.result('log_initial_balance_58')}})
        )

        log_valuetobe_gsubbed_60 = rail.PythonOperator(
            task_id='log_valuetobe_gsubbed_60',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                "value": {"number": rail.result('log_lookupbalancebasedonmonthofhire_54')}})
        )

        log_timeoff_policy_61 = rail.PythonOperator(
            task_id='log_timeoff_policy_61',
            python_callable=lambda:  json.dumps(rail.get_dag_run_var('policytoassign')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace(rail.result('log_valuefromdefault_59'), rail.result('log_valuetobe_gsubbed_60'))
        )

        log_timeoff_policy_62 = rail.PythonOperator(
            task_id='log_timeoff_policy_62',
            python_callable=lambda: rail.result('log_timeoff_policy_61').replace(rail.result('log_defaultresetamountforsickleave_45'), rail.get_dag_run_var(
                'sick_reset_amount')) if rail.result('foreach_d_26')['timeOffType']['name'] == "sick leave" else rail.result('log_timeoff_policy_61')
        )

        put_user_time_off_account_policy_set_schedule_63 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_63',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template("{{ dag_run.conf.useruri }}"),
                    "timeOffTypeUri": rail.result('foreach_d_26')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_62')
            }
        )

        log_timeoff_policy_65 = rail.PythonOperator(
            task_id='log_timeoff_policy_65',
            python_callable=lambda: rail.result('log_timeoff_policy_40').replace(rail.result('log_defaultresetamountforsickleave_45'), rail.get_dag_run_var(
                'sick_reset_amount')) if rail.result('foreach_d_26')['timeOffType']['name'] == "sick leave" else rail.result('log_timeoff_policy_40')
        )

        put_user_time_off_account_policy_set_schedule_66 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_66',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template("{{ dag_run.conf.useruri }}"),
                    "timeOffTypeUri": rail.result('foreach_d_26')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_40')
            }
        )

        put_user_time_off_account_policy_set_schedule_68 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_68',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template("{{ dag_run.conf.useruri }}"),
                    "timeOffTypeUri": rail.result('foreach_d_26')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_40')
            }
        )

        if_log_checkifthetimeoffisalreadyassigned_29_present_69 = rail.IfOperator(
            task_id='if_log_checkifthetimeoffisalreadyassigned_29_present_69',
            test='''{{ result('log_checkifthetimeoffisalreadyassigned_29') | is_truthy }}''',
            yes_task="if_name_downcase_equals_to_sickleave_70",
            no_task="foreach_d_26_end",
        )

        if_name_downcase_equals_to_sickleave_70 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_70',
            test=lambda: bool(rail.result('foreach_d_26')['timeOffType']['name'].lower(
            ) == 'sick leave' or rail.result('foreach_d_26')['timeOffType']['name'].lower() == 'floating holiday'),
            yes_task="log_lookupbalancebasedonmonthofhire_71",
            no_task="foreach_d_26_end",
        )

        log_lookupbalancebasedonmonthofhire_71 = rail.PythonOperator(
            task_id='log_lookupbalancebasedonmonthofhire_71',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == (rail.result('foreach_d_26')['timeOffType']['name']).lower() and x["monthofhire"] == dag_run.conf['startdatemonth'] and x['ftpt'] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper))[0]['balance']
        )

        if_log_lookupbalancebasedonmonthofhire_71_present_72 = rail.IfOperator(
            task_id='if_log_lookupbalancebasedonmonthofhire_71_present_72',
            test='''{{ result('log_lookupbalancebasedonmonthofhire_71') | is_truthy }}''',
            yes_task="get_default_time_off_type_policy_schedule_for_user_74",
            no_task="foreach_d_26_end",
        )

        get_default_time_off_type_policy_schedule_for_user_74 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_74',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template("{{ dag_run.conf.useruri }}"),
                    "timeOffTypeUri": rail.result('foreach_d_26')['timeOffType']['uri']
                }
            }
        )

        log_timeoff_policy_76 = rail.PythonOperator(
            task_id='log_timeoff_policy_76',
            python_callable=lambda: rail.result('get_default_time_off_type_policy_schedule_for_user_74')[
                0]['policySet'] if rail.result('get_default_time_off_type_policy_schedule_for_user_74')[0]['policySet'] else ""
        )

        if_log_timeoff_policy_76_present_77 = rail.IfOperator(
            task_id='if_log_timeoff_policy_76_present_77',
            test='''{{ result('log_timeoff_policy_76') | is_truthy }}''',
            yes_task="log_initial_balancefrompolicy_78",
            no_task="foreach_d_26_end",
        )

        log_initial_balancefrompolicy_78 = rail.PythonOperator(
            task_id='log_initial_balancefrompolicy_78',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_type_policy_schedule_for_user_74')[
                                               0]['policySet']['timeOffBalanceEventScripts'], 'script.name', "Starting Balance Set To", 'additionalParameters')).replace("[[", "[").replace("]]", "]")
        )

        parse_json_79 = rail.PythonOperator(
            task_id='parse_json_79',
            python_callable=lambda: json.loads(
                rail.result('log_initial_balancefrompolicy_78'))
        )

        log_initial_balance_80 = rail.PythonOperator(
            task_id='log_initial_balance_80',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_79'), 'keyUri', "urn:replicon:script-key:parameter:amount", 'value.number'))
        )

        log_valuefromdefault_81 = rail.PythonOperator(
            task_id='log_valuefromdefault_81',
            python_callable=lambda: {
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                    "number": rail.result('log_initial_balance_80')
                }
            }
        )

        log_valuetobe_gsubbed_82 = rail.PythonOperator(
            task_id='log_valuetobe_gsubbed_82',
            python_callable=lambda: {
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                    "number": rail.result('log_lookupbalancebasedonmonthofhire_71')
                }
            }
        )

        declare_list_83 = rail.SetVariableOperator(
            task_id='declare_list_83',
            name='count_of_new_policies',
            value=[]
        )

        declare_list_84 = rail.SetVariableOperator(
            task_id='declare_list_84',
            append=False,
            name='oldpolicyschedules',
            value=""
        )

        declare_list_85 = rail.SetVariableOperator(
            task_id='declare_list_85',
            append=False,
            name='newpolicyschedules',
            value=""
        )

        log_existing_policy_86 = rail.PythonOperator(
            task_id='log_existing_policy_86',
            python_callable=lambda:  json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_time_off_type_policy_summary_20')['policiesByTimeOffType'], 'timeOffType.uri', rail.result('foreach_d_26')['timeOffType']['uri'], 'policySetSchedule', ""))
        )

        parse_json_87 = rail.PythonOperator(
            task_id='parse_json_87',
            python_callable=lambda: json.loads(
                rail.result('log_existing_policy_86'))
        )

        foreach_document_88 = rail.ForEachOperator(
            task_id='foreach_document_88',
            items=lambda: rail.result('parse_json_87'),
            start_task='foreach_document_89',
            end_task='foreach_document_88_end'
        )

        foreach_document_89 = rail.ForEachOperator(
            task_id='foreach_document_89',
            items=lambda: rail.result('foreach_document_88'),
            start_task='log_effectivedateforcomparison_90',
            end_task='foreach_document_89_end'
        )

        log_effectivedateforcomparison_90 = rail.PythonOperator(
            task_id='log_effectivedateforcomparison_90',
            python_callable=lambda:  str(rail.result('foreach_document_89')['effectiveDate']['day']) + "/" + str(rail.result(
                'foreach_document_89')['effectiveDate']['month']) + '/' + str(rail.result('foreach_document_89')['effectiveDate']['year'])
        )

        if_to_time_less_than_todayto_time_91 = rail.IfOperator(
            task_id='if_to_time_less_than_todayto_time_91',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedateforcomparison_90'), "%d/%m/%Y").date() < now().date(),
            yes_task="insert_to_list_92",
            no_task="foreach_document_89_end",
        )

        insert_to_list_92 = rail.SetVariableOperator(
            task_id='insert_to_list_92',
            append=True,
            name='oldpolicyschedules',
            value=lambda: {
                "effectiveDate": {
                    "day": rail.result('foreach_document_89')['effectiveDate']['day'],
                    "month": rail.result('foreach_document_89')['effectiveDate']['month'],
                    "year": rail.result('foreach_document_89')['effectiveDate']['year']
                },
                "description": rail.result('foreach_document_89')['description'],
                "policySet": rail.result('foreach_document_89')['policySet']
            }
        )

        foreach_document_89_end = rail.EmptyOperator(
            task_id='foreach_document_89_end',
        )

        foreach_document_88_end = rail.EmptyOperator(
            task_id='foreach_document_88_end',
        )

        get_defaultpolicyfromgloballevel_93 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_93',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
            }
        )

        foreach_response_94 = rail.ForEachOperator(
            task_id='foreach_response_94',
            items=lambda: rail.result('get_defaultpolicyfromgloballevel_93'),
            start_task='if_startoffset_offsetvalue_greater_than_dataloggerlog_tenureoftheuser_3message_95',
            end_task='foreach_response_94_end'
        )

        if_startoffset_offsetvalue_greater_than_dataloggerlog_tenureoftheuser_3message_95 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_greater_than_dataloggerlog_tenureoftheuser_3message_95',
            test=lambda: bool(rail.result('foreach_response_94')['startOffset']['offsetValue'] > rail.result(
                'log_tenureoftheuser_3') or rail.result('foreach_response_94')['startOffset']['offsetValue'] == rail.result('log_tenureoftheuser_3')),
            yes_task="insert_to_list_96",
            no_task="foreach_response_94_end",
        )

        insert_to_list_96 = rail.SetVariableOperator(
            task_id='insert_to_list_96',
            append=True,
            name='count_of_new_policies',
            value=lambda: {
                "offset": rail.result('foreach_response_94')['startOffset']['offsetValue']
            }
        )

        foreach_response_94_end = rail.EmptyOperator(
            task_id='foreach_response_94_end',
        )

        if_declare_list_83_list_items_equals_to_0_97 = rail.IfOperator(
            task_id='if_declare_list_83_list_items_equals_to_0_97',
            test=lambda: len(rail.get_dag_run_var(
                'count_of_new_policies')) == 0,
            yes_task="get_offset_value_to_add_to_list",
            no_task="foreach_declare_list_83_101",
        )

        def get_offset_value_if_list_size_83_equals_0():
            list_93 = rail.result('get_defaultpolicyfromgloballevel_93')
            last_item = list_93[len(list_93)-1]
            return last_item['startOffset']['offsetValue']

        get_offset_value_to_add_to_list = rail.PythonOperator(
            task_id='get_offset_value_to_add_to_list',
            python_callable=get_offset_value_if_list_size_83_equals_0
        )

        insert_to_list_100 = rail.SetVariableOperator(
            task_id='insert_to_list_100',
            append=True,
            name='count_of_new_policies',
            value=lambda: {
                "offset": rail.result('get_offset_value_to_add_to_list')
            }
        )

        foreach_declare_list_83_101 = rail.ForEachOperator(
            task_id='foreach_declare_list_83_101',
            items=lambda: rail.get_dag_run_var('count_of_new_policies'),
            start_task='log_policyset_102',
            end_task='foreach_declare_list_83_101_end'
        )

        log_policyset_102 = rail.PythonOperator(
            task_id='log_policyset_102',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('get_defaultpolicyfromgloballevel_93'),
                                                                          'startOffset.offsetValue', rail.result('foreach_declare_list_83_101')['offset'], 'policySet', "")
        )

        if_foreach_declare_list_83_101_indexforeach_meta_equals_to_0_103 = rail.IfOperator(
            task_id='if_foreach_declare_list_83_101_indexforeach_meta_equals_to_0_103',
            test=lambda: rail.result('foreach_declare_list_83_101') == rail.get_dag_run_var(
                'count_of_new_policies')[0],
            yes_task="insert_to_list_104",
            no_task="log_required_effective_date_106",
        )

        insert_to_list_104 = rail.SetVariableOperator(
            task_id='insert_to_list_104',
            append=True,
            name='newpolicyschedules',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5').day }}/{{ result('invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5').month }}/{{ result('invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5').year }}",
                "policySet": lambda: rail.result('log_policyset_102')
            }
        )

        log_required_effective_date_106 = rail.PythonOperator(
            task_id='log_required_effective_date_106',
            python_callable=lambda dag_run: ((datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")) + timedelta(
                days=rail.result('foreach_declare_list_83_101')['offset'] * 365))
        )

        invoke_custom_ruby_code_required_effective_date_107 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_required_effective_date_107',
            python_callable=lambda: get_split_date(
                rail.result('log_required_effective_date_106'))
        )

        insert_to_list_108 = rail.SetVariableOperator(
            task_id='insert_to_list_108',
            append=True,
            name='newpolicyschedules',
            value=lambda: {
                "effectiveDate": {
                    "day": rail.result('invoke_custom_ruby_code_required_effective_date_107')['day'],
                    "month": rail.result('invoke_custom_ruby_code_required_effective_date_107')['month'],
                    "year": rail.result('invoke_custom_ruby_code_required_effective_date_107')['year']
                },
                "description": rail.render_template("Effective on {{ result('invoke_custom_ruby_code_required_effective_date_107').day }}/{{ result('invoke_custom_ruby_code_required_effective_date_107').month }}/{{ result('invoke_custom_ruby_code_required_effective_date_107').year }}"),
                "policySet": lambda: rail.result('log_policyset_102')
            }
        )

        foreach_declare_list_83_101_end = rail.EmptyOperator(
            task_id='foreach_declare_list_83_101_end',
        )

        log_existing_timeoff_policies_109 = rail.PythonOperator(
            task_id='log_existing_timeoff_policies_109',
            python_callable=lambda:  json.dumps(rail.get_dag_run_var('oldpolicyschedules')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace('}}]}}]', '}}]}}') if rail.get_dag_run_var('oldpolicyschedules')['policySet'] else ""
        )

        log_new_timeoff_policies_110 = rail.PythonOperator(
            task_id='log_new_timeoff_policies_110',
            python_callable=lambda: json.dumps(rail.get_dag_run_var('newpolicyschedule')).replace('null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"').replace(rail.result('log_valuefromdefault_81'), rail.result('log_valuetobe_gsubbed_82')).replace(
                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace("}}]}]}]", "}}]}}]").replace('[{"effectiveDate', '{"effectiveDate').replace('"timeOffValidationScripts":[]}]}]', '"timeOffValidationScripts":[]}}').replace('"}}]}}]', '"}}]}}') if rail.get_dag_run_var('newpolicyschedules')['policySet'] else ""
        )

        log_new_timeoff_policies_111 = rail.PythonOperator(
            task_id='log_new_timeoff_policies_111',
            python_callable=lambda:  rail.result('log_new_timeoff_policies_110').replace(rail.result('log_defaultresetamountforsickleave_45'), rail.get_dag_run_var(
                'sick_reset_amount')) if (rail.result('foreach_d_26')['timeOffType']['name']).lower() == "sick leave" else rail.result('log_new_timeoff_policies_110')
        )

        if_log_existing_timeoff_policies_109_present_112 = rail.IfOperator(
            task_id='if_log_existing_timeoff_policies_109_present_112',
            test='''{{ result('log_existing_timeoff_policies_109') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_113",
            no_task="put_user_time_off_account_policy_set_schedule_115",
        )

        put_user_time_off_account_policy_set_schedule_113 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_113',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": lambda: rail.result('foreach_d_26')['timeOffType']['uri']
                },
                "policySetScheduleEntries": json.loads("[" + rail.result('log_existing_timeoff_policies_109') + "," + rail.result('log_new_timeoff_policies_111') + "]")
            }
        )

        put_user_time_off_account_policy_set_schedule_115 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_115',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_d_26')['timeOffType']['uri']
                },
                "policySetScheduleEntries": json.loads(rail.result('log_new_timeoff_policies_111'))
            }
        )

        foreach_d_26_end = rail.EmptyOperator(
            task_id='foreach_d_26_end'
        )

        catch_errors = rail.EmptyOperator(
            task_id='catch_errors',
            trigger_rule='one_failed',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_tenureoftheuser_3
        log_tenureoftheuser_3 >> declare_list_4 >> invoke_custom_ruby_code_requireddatefortimeofftransition1st_dayofthenextmonth_5 \
            >> get_user_time_off_type_policy_summary_6 >> foreach_d_7 >> if_foreach_d_7_istimeoffallowedagainstthistimeofftype_is_true_8 \
            >> rail.Label('Yes') >> insert_to_list_9 >> foreach_d_7_end
        if_foreach_d_7_istimeoffallowedagainstthistimeofftype_is_true_8 >> rail.Label(
            'No') >> foreach_d_7_end
        foreach_d_7_end >> _adhoc_http_action_10 >> if_first_displaytext_present_11 \
            >> rail.Label('Yes') >> fdt_timeoff_balance_mapper_search_entries_12 >> declare_list_13 \
            >> foreach_fdt_timeoff_balance_mapper_search_entries_12_14 >> insert_to_list_15 >> accumulate_list_items_16 >> foreach_fdt_timeoff_balance_mapper_search_entries_12_14_end
        foreach_fdt_timeoff_balance_mapper_search_entries_12_14 >> foreach_fdt_timeoff_balance_mapper_search_entries_12_14_end >> log_final_set_timeoff_uris_17 >> if_log_12_present_18
        if_log_12_present_18 >> rail.Label('Yes') >> put_time_off_type_assignments_for_user_19 >> get_user_time_off_type_policy_summary_20\
            >> get_declare_list_4_value >> init_timeoff_dag_runs_list_21 >> foreach_declare_list_4_21 >> log_ifthetimeoff_typeisnotrequiredanymore_22 >> if_log_ifthetimeoff_typeisnotrequiredanymore_22_blank_23
        if_log_ifthetimeoff_typeisnotrequiredanymore_22_blank_23 >> rail.Label('Yes') >> get_balance_summary_for_account_24 \
            >> trigger_dag_run_timeoff_policy_update_on_each_time_off_type_for_no_accrual_025 \
            >> append_timeoff_dag_run_025 >> foreach_declare_list_4_21_end
        if_log_ifthetimeoff_typeisnotrequiredanymore_22_blank_23 >> rail.Label(
            'No') >> foreach_declare_list_4_21_end
        foreach_declare_list_4_21 >> foreach_declare_list_4_21_end >> get_timeoff_child_dag_ids_21 >> wait_for_all_timeoff_dag_runs_21 >> foreach_d_26 >> if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_27 \
            >> rail.Label('Yes') >> declare_list_28 >> log_checkifthetimeoffisalreadyassigned_29 >> get_default_time_off_policy_set_schedule_for_time_off_type_31 \
            >> foreach_response_33 >> if_foreach_response_33_indexforeach_meta_equals_to_0_34

        if_foreach_response_33_indexforeach_meta_equals_to_0_34 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_todaysdate_35 >> insert_to_list_36 >> foreach_response_33_end
        if_foreach_response_33_indexforeach_meta_equals_to_0_34 >> rail.Label(
            'No') >> invoke_custom_ruby_code_future_effective_date_38 >> insert_to_list_39 >> foreach_response_33_end
        foreach_response_33 >> foreach_response_33_end >> log_timeoff_policy_40 >> if_log_13_present_41
        if_log_13_present_41 >> rail.Label('Yes') >> log_reset_balancefrompolicy_42 >> parse_json_43 >> log_reset_balance_44 \
            >> log_defaultresetamountforsickleave_45 >> declare_variable_46 >> if_name_downcase_equals_to_sickleave_47
        if_name_downcase_equals_to_sickleave_47 >> rail.Label(
            'Yes') >> log_reset_balanceforparttime_48 >> update_variable_49 >> if_log_checkifthetimeoffisalreadyassigned_29_blank_50
        if_name_downcase_equals_to_sickleave_47 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_29_blank_50

        if_log_13_present_41 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_29_blank_50
        if_log_checkifthetimeoffisalreadyassigned_29_blank_50 >> rail.Label(
            'Yes') >> accumulate_list_items_51 >> if_log_13_present_52
        if_log_13_present_52 >> rail.Label(
            'Yes') >> if_name_downcase_equals_to_sickleave_53
        if_name_downcase_equals_to_sickleave_53 >> rail.Label(
            'Yes') >> log_lookupbalancebasedonmonthofhire_54 >> if_log_lookupbalancebasedonmonthofhire_54_present_55
        if_log_lookupbalancebasedonmonthofhire_54_present_55 >> rail.Label('Yes') >> log_initial_balancefrompolicy_56 >> parse_json_57 \
            >> log_initial_balance_58 >> log_valuefromdefault_59 >> log_valuetobe_gsubbed_60 >> log_timeoff_policy_61 >> log_timeoff_policy_62 >> put_user_time_off_account_policy_set_schedule_63 >> if_log_checkifthetimeoffisalreadyassigned_29_present_69
        if_log_lookupbalancebasedonmonthofhire_54_present_55 >> rail.Label(
            'No') >> log_timeoff_policy_65 >> put_user_time_off_account_policy_set_schedule_66 >> if_log_checkifthetimeoffisalreadyassigned_29_present_69
        if_log_13_present_52 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_29_present_69
        if_name_downcase_equals_to_sickleave_53 >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_68 >> if_log_checkifthetimeoffisalreadyassigned_29_present_69
        if_log_checkifthetimeoffisalreadyassigned_29_blank_50 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_29_present_69
        if_log_checkifthetimeoffisalreadyassigned_29_present_69 >> rail.Label(
            'Yes') >> if_name_downcase_equals_to_sickleave_70
        if_name_downcase_equals_to_sickleave_70 >> rail.Label(
            'Yes') >> log_lookupbalancebasedonmonthofhire_71 >> if_log_lookupbalancebasedonmonthofhire_71_present_72
        if_log_lookupbalancebasedonmonthofhire_71_present_72 >> rail.Label(
            'Yes') >> get_default_time_off_type_policy_schedule_for_user_74 >> log_timeoff_policy_76 >> if_log_timeoff_policy_76_present_77
        if_log_timeoff_policy_76_present_77 >> rail.Label('Yes') >> log_initial_balancefrompolicy_78 >> parse_json_79 \
            >> log_initial_balance_80 >> log_valuefromdefault_81 >> log_valuetobe_gsubbed_82 >> declare_list_83 >> declare_list_84 >> declare_list_85 \
            >> log_existing_policy_86 >> parse_json_87 >> foreach_document_88 >> foreach_document_89 >> log_effectivedateforcomparison_90 >> if_to_time_less_than_todayto_time_91
        if_to_time_less_than_todayto_time_91 >> rail.Label(
            'Yes') >> insert_to_list_92 >> foreach_document_89_end
        if_to_time_less_than_todayto_time_91 >> rail.Label(
            'No') >> foreach_document_89_end
        foreach_document_89 >> foreach_document_89_end >> foreach_document_88_end
        foreach_document_88 >> foreach_document_88_end >> get_defaultpolicyfromgloballevel_93 >> foreach_response_94 \
            >> if_startoffset_offsetvalue_greater_than_dataloggerlog_tenureoftheuser_3message_95 >> rail.Label('Yes') >> insert_to_list_96 >> foreach_response_94_end
        if_startoffset_offsetvalue_greater_than_dataloggerlog_tenureoftheuser_3message_95 >> rail.Label(
            'No') >> foreach_response_94_end
        foreach_response_94 >> foreach_response_94_end >> if_declare_list_83_list_items_equals_to_0_97

        if_declare_list_83_list_items_equals_to_0_97 >> rail.Label(
            'Yes') >> get_offset_value_to_add_to_list >> insert_to_list_100 >> foreach_declare_list_83_101
        if_declare_list_83_list_items_equals_to_0_97 >> rail.Label(
            'No') >> foreach_declare_list_83_101 >> log_policyset_102 >> if_foreach_declare_list_83_101_indexforeach_meta_equals_to_0_103
        if_foreach_declare_list_83_101_indexforeach_meta_equals_to_0_103 >> rail.Label(
            'Yes') >> insert_to_list_104 >> foreach_declare_list_83_101_end
        if_foreach_declare_list_83_101_indexforeach_meta_equals_to_0_103 >> rail.Label('No') >> log_required_effective_date_106 \
            >> invoke_custom_ruby_code_required_effective_date_107 >> insert_to_list_108 >> foreach_declare_list_83_101_end
        foreach_declare_list_83_101 >> foreach_declare_list_83_101_end >> log_existing_timeoff_policies_109
        foreach_declare_list_83_101_end >> log_existing_timeoff_policies_109 >> log_new_timeoff_policies_110 >> log_new_timeoff_policies_111 \
            >> if_log_existing_timeoff_policies_109_present_112 >> rail.Label('Yes') >> put_user_time_off_account_policy_set_schedule_113 >> foreach_d_26_end
        if_log_existing_timeoff_policies_109_present_112 >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_115 >> foreach_d_26_end

        if_log_timeoff_policy_76_present_77 >> rail.Label(
            'No') >> foreach_d_26_end
        if_log_lookupbalancebasedonmonthofhire_71_present_72 >> rail.Label(
            'No') >> foreach_d_26_end
        if_name_downcase_equals_to_sickleave_70 >> rail.Label(
            'No') >> foreach_d_26_end
        if_log_checkifthetimeoffisalreadyassigned_29_present_69 >> rail.Label(
            'No') >> foreach_d_26_end
        if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_27 >> rail.Label(
            'No') >> foreach_d_26_end
        if_log_12_present_18 >> rail.Label('No') >> catch_errors
        if_first_displaytext_present_11 >> rail.Label(
            'No') >> catch_errors

        foreach_d_26 >> foreach_d_26_end >> catch_errors >> finish

    return dag


rail.for_each_instance(create_dag)
