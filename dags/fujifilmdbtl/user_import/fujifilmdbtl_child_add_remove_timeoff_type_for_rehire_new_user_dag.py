
from datetime import timedelta, datetime
from pendulum import now
import json
from airflow.models import Variable
import rail
from fujifilmdbtl.user_import.mapper.fujifilmdbtl_timeoff_balance_mapper import fdt_timeoff_balance_mapper
from fujifilmdbtl.user_import.utils.python_callable import get_split_date

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_child_add_remove_timeoff_type_for_rehire_new_user_{config.instance}',
        description=f'FDT Child Workflow to add/remove timeoff type for Rehire - New User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_3',
            end_task='catch_error_143',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='assigned_timeoff_types',
            value=[]
        )

        invoke_custom_ruby_code_todaysdate_4 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_todaysdate_4',
            python_callable=lambda: (datetime.now()).isoformat()
        )

        get_user_time_off_type_policy_summary_5 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_5',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        foreach_d_6 = rail.ForEachOperator(
            task_id='foreach_d_6',
            items=lambda: rail.result('get_user_time_off_type_policy_summary_5')[
                'policiesByTimeOffType'],
            start_task='if_foreach_d_6_istimeoffallowedagainstthistimeofftype_is_true_7',
            end_task='foreach_d_6_end'
        )

        if_foreach_d_6_istimeoffallowedagainstthistimeofftype_is_true_7 = rail.IfOperator(
            task_id='if_foreach_d_6_istimeoffallowedagainstthistimeofftype_is_true_7',
            test=lambda: bool(rail.result('foreach_d_6')[
                              'isTimeOffAllowedAgainstThisTimeOffType']),
            yes_task="insert_to_list_8",
            no_task="foreach_d_6_end",
        )

        insert_to_list_8 = rail.SetVariableOperator(
            task_id='insert_to_list_8',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "name": "{{ result('foreach_d_6').timeOffType.name }}",
                "uri": "{{ result('foreach_d_6').timeOffType.uri }}",
                "policyset": "{{ result('foreach_d_6').policySetSchedule')"
            }
        )

        foreach_d_6_end = rail.EmptyOperator(
            task_id='foreach_d_6_end',
        )

        _adhoc_http_action_9 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_9',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        if_first_displaytext_present_10 = rail.IfOperator(
            task_id='if_first_displaytext_present_10',
            test=lambda: bool(rail.result(
                '_adhoc_http_action_9')[0]['displayText']),
            yes_task="fdt_timeoff_balance_mapper_search_entries_11",
            no_task="catch_error_121",
        )

        fdt_timeoff_balance_mapper_search_entries_11 = rail.PythonOperator(
            task_id='fdt_timeoff_balance_mapper_search_entries_11',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "timeoff" and x["monthofhire"] == dag_run.conf['regulartemp'] and x["ftpt"] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper))
        )

        declare_list_12 = rail.SetVariableOperator(
            task_id='declare_list_12',
            append=False,
            name='timeoff_types_to_assign',
            value=[]
        )

        foreach_fdt_timeoff_balance_mapper_search_entries_11_13 = rail.ForEachOperator(
            task_id='foreach_fdt_timeoff_balance_mapper_search_entries_11_13',
            items=lambda: rail.result(
                'fdt_timeoff_balance_mapper_search_entries_11'),
            start_task='insert_to_list_14',
            end_task='foreach_fdt_timeoff_balance_mapper_search_entries_11_13_end'
        )

        insert_to_list_14 = rail.SetVariableOperator(
            task_id='insert_to_list_14',
            append=True,
            name='{{ result("declare_list_12").name }}',
            value=lambda: {
                "name": rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_11_13')['balance'],
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_9'), 'displayText', rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_11_13')["balance"], 'uri', "")
            }
        )

        accumulate_list_items_15 = rail.SetVariableOperator(
            task_id='accumulate_list_items_15',
            name='logging',
            append=True,
            value=lambda: {
                "name": rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_11_13')['balance'],
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_9'), 'displayText', rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_11_13')["balance"], 'uri', "")
            }
        )

        foreach_fdt_timeoff_balance_mapper_search_entries_11_13_end = rail.EmptyOperator(
            task_id='foreach_fdt_timeoff_balance_mapper_search_entries_11_13_end'
        )

        log_final_set_timeoff_uris_16 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_16',
            python_callable=lambda:  rail.smartjoin_by_delim([x['uri'] for x in rail.get_dag_run_var(
                'timeoff_types_to_assign')], ',') if rail.get_dag_run_var('timeoff_types_to_assign')['name'] else ""
        )

        if_log_12_present_17 = rail.IfOperator(
            task_id='if_log_12_present_17',
            test='''{{ result('log_final_set_timeoff_uris_16') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_18",
            no_task="catch_error_121",
        )

        put_time_off_type_assignments_for_user_18 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_18',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUris": [
                    "{{ result('log_final_set_timeoff_uris_16') }}"
                ]
            }
        )

        get_user_time_off_type_policy_summary_19 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_19',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        # Initialize variable to collect DAG runs from foreach loop
        init_timeoff_dag_runs_list_20 = rail.SetVariableOperator(
            task_id='init_timeoff_dag_runs_list_20',
            name='timeoff_dag_runs_20',
            append=False,
            value=[]
        )

        foreach_declare_list_3_20 = rail.ForEachOperator(
            task_id='foreach_declare_list_3_20',
            items=lambda: rail.get_dag_run_var('assigned_timeoff_types'),
            start_task='log_ifthetimeoff_typeisnotrequiredanymore_21',
            end_task='foreach_declare_list_3_20_end'
        )

        log_ifthetimeoff_typeisnotrequiredanymore_21 = rail.PythonOperator(
            task_id='log_ifthetimeoff_typeisnotrequiredanymore_21',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('logging'), 'uri', rail.result(
                'foreach_declare_list_3_20')['uri']) if rail.get_dag_run_var('logging')[0]['name'] else ""
        )

        if_log_ifthetimeoff_typeisnotrequiredanymore_21_blank_22 = rail.IfOperator(
            task_id='if_log_ifthetimeoff_typeisnotrequiredanymore_21_blank_22',
            test='''{{ result('log_ifthetimeoff_typeisnotrequiredanymore_21') | is_falsy }}''',
            yes_task="get_balance_summary_for_account_23",
            no_task="foreach_d_25",
        )

        get_balance_summary_for_account_23 = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account_23',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data={
                "account": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_declare_list_3_20').uri }}"
                },
                "asOfDate": {
                    "year": "{{ result('invoke_custom_ruby_code_todaysdate_4').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_todaysdate_4').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_todaysdate_4').day }}"
                }
            }
        )

        trigger_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_024 = rail.TriggerDagRunOperator(
            task_id='trigger_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_024',
            retries=0,
            trigger_dag_id=f'fujifilmdbtl_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('foreach_declare_list_3_20').uri }}",
                "policyset": lambda: json.loads(rail.result('foreach_declare_list_3_20.policyset')),
                "enddate": now().strftime("%d/%m/%Y"),
                "newschedulebalance": "{{ result('get_balance_summary_for_account_23').timeRemaining }}"
            }
        )

        # Append the triggered DAG run to the collection variable
        append_timeoff_dag_run_024 = rail.SetVariableOperator(
            task_id='append_timeoff_dag_run_024',
            name='timeoff_dag_runs_20',
            append=True,
            value='{{ result("trigger_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_024") }}'
        )

        foreach_declare_list_3_20_end = rail.EmptyOperator(
            task_id='foreach_declare_list_3_20_end'
        )

        # Process the collected DAG run IDs
        get_timeoff_child_dag_ids_20 = rail.PythonOperator(
            task_id='get_timeoff_child_dag_ids_20',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('timeoff_dag_runs_20')] if rail.get_dag_run_var('timeoff_dag_runs_20') else []
        )

        # Wait for all collected DAG runs after the foreach loop completes
        wait_for_all_timeoff_dag_runs_20 = rail.WaitForDagRunsSensor(
            task_id='wait_for_all_timeoff_dag_runs_20',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_timeoff_child_dag_ids_20') | to_json }}"
        )

        foreach_d_25 = rail.ForEachOperator(
            task_id='foreach_d_25',
            items=lambda: rail.result('get_user_time_off_type_policy_summary_19')[
                'policiesByTimeOffType'],
            start_task='if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_26',
            end_task='foreach_d_25_end'
        )

        if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_26 = rail.IfOperator(
            task_id='if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_26',
            test=lambda: bool(rail.result('foreach_d_25')[
                              'isTimeOffAllowedAgainstThisTimeOffType']),
            yes_task="log_checkifthetimeoffisalreadyassigned_27",
            no_task="catch_error_121",
        )

        log_checkifthetimeoffisalreadyassigned_27 = rail.PythonOperator(
            task_id='log_checkifthetimeoffisalreadyassigned_27',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('Assigned Timeoff Type'), 'uri', rail.result(
                'foreach_d_25.timeOffType')['uri']) if rail.get_dag_run_var('Assigned Timeoff Type')[0]['name'] else ""
        )

        get_default_time_off_type_policy_schedule_for_user_29 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_29',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": lambda: rail.result('foreach_d_25')['timeOffType']['uri']
                }
            }
        )

        catch_121_121_30 = rail.EmptyOperator(
            task_id='catch_121_121_30',
            trigger_rule='one_failed',
        )

        log_timeoff_policy_31 = rail.PythonOperator(
            task_id='log_timeoff_policy_31',
            python_callable=lambda: (json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_29')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"')) if rail.result('get_default_time_off_type_policy_schedule_for_user_29')[0]['policySet'] else ""
        )

        if_log_timeoff_policy_31_present_32 = rail.IfOperator(
            task_id='if_log_timeoff_policy_31_present_32',
            test='''{{ result('log_timeoff_policy_31') | is_truthy }}''',
            yes_task="log_reset_balancefrompolicy_33",
            no_task="if_log_checkifthetimeoffisalreadyassigned_27_blank_41",
        )

        log_reset_balancefrompolicy_33 = rail.PythonOperator(
            task_id='log_reset_balancefrompolicy_33',
            python_callable=lambda:  json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_type_policy_schedule_for_user_29')[
                                                0]['policySet']['timeOffBalanceEventScripts'], 'script.name', "Yearly Reset", 'additionalParameters', "")).replace("[[", "[").replace("]]", "]")
        )

        parse_json_34 = rail.PythonOperator(
            task_id='parse_json_34',
            python_callable=lambda: json.loads(
                rail.result('log_reset_balancefrompolicy_33'))
        )

        log_reset_balance_35 = rail.PythonOperator(
            task_id='log_reset_balance_35',
            python_callable=lambda: str(float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_34'), 'keyUri', "urn:replicon:script-key:parameter:reset-balance-amount", 'value.number', "")))
        )

        log_defaultresetamount_36 = rail.PythonOperator(
            task_id='log_defaultresetamount_36',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                                                "value": {"number": rail.result('log_reset_balance_35')}})
        )

        declare_variable_37 = rail.SetVariableOperator(
            task_id='declare_variable_37',
            append=False,
            name='sick_reset_amount',
            value=""
        )

        if_name_downcase_equals_to_sickleave_38 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_38',
            test=lambda dag_run: rail.result('foreach_d_25')['timeOffType']['name'].lower(
            ) == 'sick leave' and dag_run.conf.ftpt == 'p',
            yes_task="log_reset_balanceforparttime_39",
            no_task="if_log_checkifthetimeoffisalreadyassigned_27_blank_41",
        )

        log_reset_balanceforparttime_39 = rail.PythonOperator(
            task_id='log_reset_balanceforparttime_39',
            python_callable=lambda: str(
                float(rail.result('log_reset_balance_35')) / 2)
        )

        update_variable_40 = rail.SetVariableOperator(
            task_id='update_variable_40',
            append=False,
            name='sick_reset_amount',
            value=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                                      "value": {"number": rail.result('log_reset_balanceforparttime_39')}})
        )

        if_log_checkifthetimeoffisalreadyassigned_27_blank_41 = rail.IfOperator(
            task_id='if_log_checkifthetimeoffisalreadyassigned_27_blank_41',
            test='''{{ result('log_checkifthetimeoffisalreadyassigned_27') | is_falsy }}''',
            yes_task="accumulate_list_items_42",
            no_task="if_log_checkifthetimeoffisalreadyassigned_27_present_60",
        )

        accumulate_list_items_42 = rail.SetVariableOperator(
            task_id='accumulate_list_items_42',
            name='assigned_timeoff_types',
            append=True,
            value={
                "timeofftype": "{{ result('foreach_d_25').timeOffType.name }}"
            }
        )

        if_log_timeoff_policy_31_present_43 = rail.IfOperator(
            task_id='if_log_timeoff_policy_31_present_43',
            test='''{{ result('log_timeoff_policy_31') | is_truthy }}''',
            yes_task="if_name_downcase_equals_to_sickleave_44",
            no_task="if_log_checkifthetimeoffisalreadyassigned_27_present_60",
        )

        if_name_downcase_equals_to_sickleave_44 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_44',
            test=lambda: rail.result('foreach_d_25')['timeOffType']['name'].lower() == 'sick leave' or rail.result(
                'foreach_d_25')['timeOffType']['name'].lower() == 'floating holiday',
            yes_task="log_lookupbalancebasedonmonthofhire_45",
            no_task="put_user_time_off_account_policy_set_schedule_59",
        )

        log_lookupbalancebasedonmonthofhire_45 = rail.PythonOperator(
            task_id='log_lookupbalancebasedonmonthofhire_45',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == rail.result('foreach_d_25')['timeOffType']['name'].lower(
            ) and x["monthofhire"] == dag_run.conf['startdatemonth'] and x["ftpt"] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper))[0]['balance']
        )

        if_log_lookupbalancebasedonmonthofhire_45_present_46 = rail.IfOperator(
            task_id='if_log_lookupbalancebasedonmonthofhire_45_present_46',
            test='''{{ result('log_lookupbalancebasedonmonthofhire_45') | is_truthy }}''',
            yes_task="log_initial_balancefrompolicy_47",
            no_task="log_timeoff_policy_56",
        )

        log_initial_balancefrompolicy_47 = rail.PythonOperator(
            task_id='log_initial_balancefrompolicy_47',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_type_policy_schedule_for_user_29')[
                                               0]['policySet']['timeOffBalanceEventScripts'], 'script.name', "Starting Balance Set To", 'additionalParameters', "")).replace("[[", "[").replace("]]", "]")
        )

        parse_json_48 = rail.PythonOperator(
            task_id='parse_json_48',
            python_callable=lambda: json.loads(
                rail.result('log_initial_balancefrompolicy_47'))
        )

        log_initial_balance_49 = rail.PythonOperator(
            task_id='log_initial_balance_49',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_48'), 'keyUri', "urn:replicon:script-key:parameter:amount", 'value.number'))
        )

        log_valuefromdefault_50 = rail.PythonOperator(
            task_id='log_valuefromdefault_50',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                "value": {"number": rail.result('log_initial_balance_49')}})
        )

        log_valuetobe_gsubbed_51 = rail.PythonOperator(
            task_id='log_valuetobe_gsubbed_51',
            python_callable=lambda:  json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                 "value": {"number": rail.result('log_lookupbalancebasedonmonthofhire_45')}})
        )

        log_timeoff_policy_52 = rail.PythonOperator(
            task_id='log_timeoff_policy_52',
            python_callable=lambda:  json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_29')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace(rail.result('log_valuefromdefault_50'), rail.result('log_valuetobe_gsubbed_51'))
        )

        log_timeoff_policy_53 = rail.PythonOperator(
            task_id='log_timeoff_policy_53',
            python_callable=lambda: rail.result('log_timeoff_policy_52').replace(rail.result('log_defaultresetamount_36'), rail.get_dag_run_var(
                'sick_reset_amount')) if rail.result('foreach_d_25')['timeOffType']['name'].lower() == 'sick leave' else rail.result('log_timeoff_policy_52')
        )

        put_user_time_off_account_policy_set_schedule_54 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_54',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_d_25')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_53')
            }
        )

        log_timeoff_policy_56 = rail.PythonOperator(
            task_id='log_timeoff_policy_56',
            python_callable=lambda: rail.result('log_timeoff_policy_31').replace(rail.result('log_defaultresetamount_36'), rail.get_dag_run_var(
                'sick_reset_amount')) if rail.result('foreach_d_25')['timeOffType']['name'].lower() == 'sick leave' else rail.result('log_timeoff_policy_31')
        )

        put_user_time_off_account_policy_set_schedule_57 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_57',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_d_25')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_56')
            }
        )

        put_user_time_off_account_policy_set_schedule_59 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_59',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_d_25')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_31')
            }
        )

        if_log_checkifthetimeoffisalreadyassigned_27_present_60 = rail.IfOperator(
            task_id='if_log_checkifthetimeoffisalreadyassigned_27_present_60',
            test='''{{ result('log_checkifthetimeoffisalreadyassigned_27') | is_truthy }}''',
            yes_task="if_name_downcase_equals_to_sickleave_61",
            no_task="catch_error_121",
        )

        if_name_downcase_equals_to_sickleave_61 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_61',
            test=lambda: rail.result('foreach_d_25')['timeOffType']['name'].lower() == 'sick leave' or rail.result(
                'foreach_d_25')['timeOffType']['name'].lower() == 'floating holiday',
            yes_task="log_lookupbalancebasedonmonthofhire_62",
            no_task="if_name_downcase_not_equals_to_sickleave_101",
        )

        log_lookupbalancebasedonmonthofhire_62 = rail.PythonOperator(
            task_id='log_lookupbalancebasedonmonthofhire_62',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == rail.result('foreach_d_25')['timeOffType']['name'].lower(
            ) and x["monthofhire"] == dag_run.conf['startdatemonth'] and x["ftpt"] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper))[0]['balance']
        )

        if_log_lookupbalancebasedonmonthofhire_62_present_63 = rail.IfOperator(
            task_id='if_log_lookupbalancebasedonmonthofhire_62_present_63',
            test='''{{ result('log_lookupbalancebasedonmonthofhire_62') | is_truthy }}''',
            yes_task="get_default_time_off_type_policy_schedule_for_user_65",
            no_task="if_name_downcase_not_equals_to_sickleave_101",
        )

        get_default_time_off_type_policy_schedule_for_user_65 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_65',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_d_25')['timeOffType']['uri']
                }
            }
        )

        log_timeoff_policy_67 = rail.PythonOperator(
            task_id='log_timeoff_policy_67',
            python_callable=lambda:  rail.result('get_default_time_off_type_policy_schedule_for_user_65')[
                0]['policySet'] if rail.result('get_default_time_off_type_policy_schedule_for_user_65')[0]['policySet'] else ""
        )

        if_log_timeoff_policy_67_present_68 = rail.IfOperator(
            task_id='if_log_timeoff_policy_67_present_68',
            test='''{{ result('log_timeoff_policy_67') | is_truthy }}''',
            yes_task="log_initial_balancefrompolicy_69",
            no_task="if_name_downcase_not_equals_to_sickleave_101",
        )

        log_initial_balancefrompolicy_69 = rail.PythonOperator(
            task_id='log_initial_balancefrompolicy_69',
            python_callable=lambda:  json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_type_policy_schedule_for_user_65')[
                                                0]['policySet']['timeOffBalanceEventScripts'], 'script.name', "Starting Balance Set To", 'additionalParameters', "")).replace("[[", "[").replace("]]", "]")
        )

        parse_json_70 = rail.PythonOperator(
            task_id='parse_json_70',
            python_callable=lambda: json.loads(
                rail.result('log_initial_balancefrompolicy_69'))
        )

        log_initial_balance_71 = rail.PythonOperator(
            task_id='log_initial_balance_71',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_70'), 'keyUri', "urn:replicon:script-key:parameter:amount", 'value.number'))
        )

        log_valuefromdefault_72 = rail.PythonOperator(
            task_id='log_valuefromdefault_72',
            python_callable=lambda:  json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                 "value": {"number": rail.result('log_initial_balance_71')}})
        )

        log_valuetobe_gsubbed_73 = rail.PythonOperator(
            task_id='log_valuetobe_gsubbed_73',
            python_callable=lambda:  json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                 "value": {"number": rail.result('log_lookupbalancebasedonmonthofhire_62')}})
        )

        declare_list_74 = rail.SetVariableOperator(
            task_id='declare_list_74',
            append=False,
            name='oldpolicyschedules',
            value=[]
        )

        declare_list_75 = rail.SetVariableOperator(
            task_id='declare_list_75',
            append=False,
            name='newpolicyschedules',
            value=[]
        )

        log_existing_policy_76 = rail.PythonOperator(
            task_id='log_existing_policy_76',
            python_callable=lambda:  json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_type_policy_summary_19')[
                                                'policiesByTimeOffType'], 'timeOffType.uri', rail.result('foreach_d_25.timeOffType')['uri'], 'policySetSchedule', ""))
        )

        if_log_existing_policy_76_present_77 = rail.IfOperator(
            task_id='if_log_existing_policy_76_present_77',
            test='''{{ result('log_existing_policy_76') | is_truthy }}''',
            yes_task="parse_json_78",
            no_task="get_defaultpolicyfromgloballevel_84",
        )

        parse_json_78 = rail.PythonOperator(
            task_id='parse_json_78',
            python_callable=lambda: json.loads(
                rail.result('log_existing_policy_76'))
        )

        foreach_document_79 = rail.ForEachOperator(
            task_id='foreach_document_79',
            items=lambda: rail.result('parse_json_78'),
            start_task='foreach_foreach_document_79_80',
            end_task='foreach_document_79_end'
        )

        foreach_foreach_document_79_80 = rail.ForEachOperator(
            task_id='foreach_foreach_document_79_80',
            items=lambda: rail.result('foreach_document_79'),
            start_task='log_effectivedateforcomparison_81',
            end_task='foreach_foreach_document_79_80_end'
        )

        log_effectivedateforcomparison_81 = rail.PythonOperator(
            task_id='log_effectivedateforcomparison_81',
            python_callable=lambda:  "{{ result('foreach_foreach_document_79_80').effectiveDate.day }}/{{ result('foreach_foreach_document_79_80').effectiveDate.month }}/{{ result('foreach_foreach_document_79_80').effectiveDate.year }}"
        )

        if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_82 = rail.IfOperator(
            task_id='if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_82',
            test=lambda dag_run:  datetime.strptime(rail.result('log_effectivedateforcomparison_81'), "%d/%m/%Y"
            ) < datetime.strptime(dag_run.conf['rehiredate'], "%m/%d/%Y"),
            yes_task="insert_to_list_83",
            no_task="foreach_foreach_document_79_80_end",
        )

        insert_to_list_83 = rail.SetVariableOperator(
            task_id='insert_to_list_83',
            append=True,
            name='oldpolicyschedules',
            value={
                "effectiveDate": {
                    "day": "{{ result('foreach_foreach_document_79_80').effectiveDate.day }}",
                    "month": "{{ result('foreach_foreach_document_79_80').effectiveDate.month }}",
                    "year": "{{ result('foreach_foreach_document_79_80').effectiveDate.year }}"
                },
                "description": "{{ result('foreach_foreach_document_79_80').description }}",
                "policySet": "{{result('foreach_foreach_document_79_80').policySet}}"
            }
        )

        foreach_foreach_document_79_80_end = rail.EmptyOperator(
            task_id='foreach_foreach_document_79_80_end',
        )

        foreach_document_79_end = rail.EmptyOperator(
            task_id='foreach_document_79_end',
        )

        get_defaultpolicyfromgloballevel_84 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_84',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('foreach_d_25')['timeOffType']['uri']
            }
        )

        foreach_response_85 = rail.ForEachOperator(
            task_id='foreach_response_85',
            items=lambda: rail.result('get_defaultpolicyfromgloballevel_84'),
            start_task='log_policyset_86',
            end_task='foreach_response_85_end'
        )

        log_policyset_86 = rail.PythonOperator(
            task_id='log_policyset_86',
            python_callable=lambda:  rail.result(
                'foreach_response_85')['policySet']
        )

        if_foreach_response_85_indexforeach_meta_equals_to_0_87 = rail.IfOperator(
            task_id='if_foreach_response_85_indexforeach_meta_equals_to_0_87',
            test='''{{ result('foreach_response_85').index == 0 }}''',
            yes_task="invoke_custom_ruby_code_rehire_date_88",
            no_task="log_required_effective_date_91",
        )

        invoke_custom_ruby_code_rehire_date_88 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_rehire_date_88',
            python_callable=lambda dag_run: get_split_date(
                dag_run.conf['rehiredate'], "%m/%d/%Y")
        )

        insert_to_list_89 = rail.SetVariableOperator(
            task_id='insert_to_list_89',
            append=True,
            name='newpolicyscheduleslist',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_rehire_date_88').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_rehire_date_88').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_rehire_date_88').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_rehire_date_88').day }}/{{ result('invoke_custom_ruby_code_rehire_date_88').month }}/{{ result('invoke_custom_ruby_code_rehire_date_88').year }}",
                "policySet": "{{result('log_policyset_86')}}"
            }
        )

        log_required_effective_date_91 = rail.PythonOperator(
            task_id='log_required_effective_date_91',
            python_callable=lambda dag_run:  (datetime.strptime(dag_run.conf['rehiredate'], "%m/%d/%Y") + timedelta(
                days=(rail.result('foreach_response_85')['startOffset']['offsetValue'])*365)).strftime("%m/%d/%Y")
        )

        invoke_custom_ruby_code_required_effective_date_92 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_required_effective_date_92',
            python_callable=lambda: get_split_date(
                rail.result('log_required_effective_date_91'))
        )

        insert_to_list_93 = rail.SetVariableOperator(
            task_id='insert_to_list_93',
            append=True,
            name='newpolicyschedules',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_required_effective_date_92').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_required_effective_date_92').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_required_effective_date_92').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_required_effective_date_92').day }}/{{ result('invoke_custom_ruby_code_required_effective_date_92').month }}/{{ result('invoke_custom_ruby_code_required_effective_date_92').year }}",
                "policySet": "{{result('log_policyset_86')}}"
            }
        )

        foreach_response_85_end = rail.EmptyOperator(
            task_id='foreach_response_85_end',
        )

        log_existing_timeoff_policies_94 = rail.PythonOperator(
            task_id='log_existing_timeoff_policies_94',
            python_callable=lambda:  json.dumps(rail.get_dag_run_var('oldpolicyschedules')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace('}}]}}]', '}}]}}') if rail.get_dag_run_var('oldpolicyschedules')['policySet'] else ""
        )

        log_new_timeoff_policies_95 = rail.PythonOperator(
            task_id='log_new_timeoff_policies_95',
            python_callable=lambda: json.dumps(rail.get_dag_run_var('newpolicyschedule')).replace('null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"').replace(rail.result('log_valuefromdefault_72'), rail.result('log_valuetobe_gsubbed_73')).replace(
                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace("}}]}]}]", "}}]}}]").replace('[{"effectiveDate', '{"effectiveDate').replace('"timeOffValidationScripts":[]}]}]', '"timeOffValidationScripts":[]}}').replace('"}}]}}]', '"}}]}}') if rail.get_dag_run_var('newpolicyschedules')['policySet'] else ""
        )

        log_new_timeoff_policies_96 = rail.PythonOperator(
            task_id='log_new_timeoff_policies_96',
            python_callable=lambda:  rail.result('log_new_timeoff_policies_95').replace(rail.result('log_defaultresetamount_36'), rail.get_dag_run_var(
                'sick_reset_amount')) if rail.result('foreach_d_25')['timeOffType']['name'].lower() == "sick leave" else rail.result('log_new_timeoff_policies_95')
        )

        if_log_existing_timeoff_policies_94_present_97 = rail.IfOperator(
            task_id='if_log_existing_timeoff_policies_94_present_97',
            test='''{{ result('log_existing_timeoff_policies_94') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_98",
            no_task="if_log_existing_timeoff_policies_94_blank_99",
        )

        put_user_time_off_account_policy_set_schedule_98 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_98',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_25').timeOffType.uri }}"
                },
                "policySetScheduleEntries": {"{{ result('log_existing_timeoff_policies_94') }}", "{{ result('log_new_timeoff_policies_96') }}"}
            }
        )

        if_log_existing_timeoff_policies_94_blank_99 = rail.IfOperator(
            task_id='if_log_existing_timeoff_policies_94_blank_99',
            test='''{{ result('log_existing_timeoff_policies_94') | is_falsy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_100",
            no_task="if_name_downcase_not_equals_to_sickleave_101",
        )

        put_user_time_off_account_policy_set_schedule_100 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_100',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_25').timeOffType.uri }}"
                },
                "policySetScheduleEntries": "{{ result('log_new_timeoff_policies_96') }}"
            }
        )

        if_name_downcase_not_equals_to_sickleave_101 = rail.IfOperator(
            task_id='if_name_downcase_not_equals_to_sickleave_101',
            test=lambda: rail.result('foreach_d_25')['timeOffType']['name'].lower() == 'sick leave' and rail.result(
                'foreach_d_25')['timeOffType']['name'].lower() == 'floating holiday',
            yes_task="get_defaultpolicyfromgloballevel_102",
            no_task="catch_error_121",
        )

        get_defaultpolicyfromgloballevel_102 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_102',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('foreach_d_25')['timeOffType']['uri']
            }
        )

        log_timeoff_policy_103 = rail.PythonOperator(
            task_id='log_timeoff_policy_103',
            python_callable=lambda:  rail.result(
                'get_defaultpolicyfromgloballevel_102')[0]['policySet']
        )

        if_log_timeoff_policy_103_present_104 = rail.IfOperator(
            task_id='if_log_timeoff_policy_103_present_104',
            test='''{{ result('log_timeoff_policy_103') | is_truthy }}''',
            yes_task="declare_list_105",
            no_task="log_timeoff_policies_118",
        )

        declare_list_105 = rail.SetVariableOperator(
            task_id='declare_list_105',
            append=False,
            name='policyschedules',
            value=[]
        )

        log_existing_policy_106 = rail.PythonOperator(
            task_id='log_existing_policy_106',
            python_callable=lambda:  json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_type_policy_summary_19')[
                                                'policiesByTimeOffType'], 'timeOffType.uri', rail.result('foreach_d_25')['timeOffType']['uri'], 'policySetSchedule', ""))
        )

        if_log_existing_policy_106_present_107 = rail.IfOperator(
            task_id='if_log_existing_policy_106_present_107',
            test='''{{ result('log_existing_policy_106') | is_truthy }}''',
            yes_task="parse_json_108",
            no_task="foreach_response_114",
        )

        parse_json_108 = rail.PythonOperator(
            task_id='parse_json_108',
            python_callable=lambda: json.loads(
                rail.result('log_existing_policy_106'))
        )

        foreach_document_109 = rail.ForEachOperator(
            task_id='foreach_document_109',
            items=lambda: rail.result('parse_json_108'),
            start_task='foreach_foreach_document_109_110',
            end_task='foreach_document_109_end'
        )

        foreach_foreach_document_109_110 = rail.ForEachOperator(
            task_id='foreach_foreach_document_109_110',
            items=lambda: rail.result('foreach_document_109'),
            start_task='log_effectivedateforcomparison_111',
            end_task='foreach_foreach_document_109_110_end'
        )

        log_effectivedateforcomparison_111 = rail.PythonOperator(
            task_id='log_effectivedateforcomparison_111',
            python_callable=lambda:  str(rail.result('foreach_foreach_document_109_110')['effectiveDate']['day']) + "/" + str(rail.result(
                'foreach_foreach_document_109_110')['effectiveDate']['month']) + "/" + str(rail.result('foreach_foreach_document_109_110')['effectiveDate']['year'])
        )

        if_effective_date_for_comparison_less_than_rehire_date_112 = rail.IfOperator(
            task_id='if_effective_date_for_comparison_less_than_rehire_date_112',
            test=lambda dag_run: datetime.strptime(rail.result('log_effectivedateforcomparison_111'), "%d/%m/%Y").date() < datetime.strptime(
                dag_run.conf['rehiredate'], "%m/%d/%Y").date(),
            yes_task="insert_to_list_113",
            no_task="foreach_foreach_response_110_end",
        )

        insert_to_list_113 = rail.SetVariableOperator(
            task_id='insert_to_list_113',
            append=True,
            name='{{ result("declare_list_105").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('foreach_foreach_document_109_110').effectiveDate.day }}",
                    "month": "{{ result('foreach_foreach_document_109_110').effectiveDate.month }}",
                    "year": "{{ result('foreach_foreach_document_109_110').effectiveDate.year }}"
                },
                "description": "{{ result('foreach_foreach_document_109_110').description }}",
                "policySet": "{{result('foreach_foreach_document_109_110').policySet)}}"
            }
        )

        foreach_foreach_response_110_end = rail.EmptyOperator(
            task_id='foreach_foreach_response_110_end',
        )

        foreach_response_109_end = rail.EmptyOperator(
            task_id='foreach_response_109_end',
        )

        foreach_response_114 = rail.ForEachOperator(
            task_id='foreach_response_114',
            items=lambda: rail.result('get_defaultpolicyfromgloballevel_102'),
            start_task='log_required_effective_date_115',
            end_task='foreach_response_114_end'
        )

        log_required_effective_date_115 = rail.PythonOperator(
            task_id='log_required_effective_date_115',
            python_callable=lambda dag_run: datetime.strptime(dag_run.conf['rehiredate'], "%m/%d/%Y") + timedelta(
                days=(rail.result('foreach_response_85')['startOffset']['offsetValue'])*365)
        )

        invoke_custom_ruby_code_required_effective_date_116 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_required_effective_date_116',
            python_callable=lambda: get_split_date(
                rail.result('log_required_effective_date_115'))
        )

        insert_to_list_117 = rail.SetVariableOperator(
            task_id='insert_to_list_117',
            append=True,
            name='policyscheduleslist',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_required_effective_date_116').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_required_effective_date_116').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_required_effective_date_116').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_required_effective_date_116').day }}/{{ result('invoke_custom_ruby_code_required_effective_date_116').month }}/{{ result('invoke_custom_ruby_code_required_effective_date_116').year }}",
                "policySet": "{{result('foreach_response_114').policySet)}}"
            }
        )

        foreach_response_114_end = rail.EmptyOperator(
            task_id='foreach_response_114_end',
        )

        log_timeoff_policies_118 = rail.PythonOperator(
            task_id='log_timeoff_policies_118',
            python_callable=lambda: json.dumps(rail.get_dag_run_var('policyschedules')).replace('null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"').replace(
                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace("}}]}]}]", "}}]}}]") if rail.get_dag_run_var('policyschedules')['policySet'] else ""
        )

        if_log_timeoff_policies_118_present_119 = rail.IfOperator(
            task_id='if_log_timeoff_policies_118_present_119',
            test='''{{ result('log_timeoff_policies_118') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_120",
            no_task="catch_error_121",
        )

        put_user_time_off_account_policy_set_schedule_120 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_120',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_d_25')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policies_118')
            }
        )

        foreach_d_25_end = rail.EmptyOperator(
            task_id='foreach_d_25_end'
        )

        catch_error_121 = rail.EmptyOperator(
            task_id='catch_error_121',
            trigger_rule='one_failed',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error_121
        can_run_batch_task >> rail.Label('No') >> declare_list_3
        declare_list_3 >> invoke_custom_ruby_code_todaysdate_4 >> get_user_time_off_type_policy_summary_5 >> foreach_d_6 \
            >> if_foreach_d_6_istimeoffallowedagainstthistimeofftype_is_true_7
        if_foreach_d_6_istimeoffallowedagainstthistimeofftype_is_true_7 >> rail.Label(
            'Yes') >> insert_to_list_8 >> foreach_d_6_end
        if_foreach_d_6_istimeoffallowedagainstthistimeofftype_is_true_7 >> rail.Label(
            'No') >> foreach_d_6_end
        foreach_d_6 >> foreach_d_6_end >> _adhoc_http_action_9 >> if_first_displaytext_present_10
        if_first_displaytext_present_10 >> rail.Label('Yes') >> fdt_timeoff_balance_mapper_search_entries_11 >> declare_list_12 \
            >> foreach_fdt_timeoff_balance_mapper_search_entries_11_13 >> insert_to_list_14 >> accumulate_list_items_15 \
            >> foreach_fdt_timeoff_balance_mapper_search_entries_11_13_end >> log_final_set_timeoff_uris_16 >> if_log_12_present_17
        if_log_12_present_17 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_18 >> get_user_time_off_type_policy_summary_19 >> init_timeoff_dag_runs_list_20 \
            >> foreach_declare_list_3_20 >> log_ifthetimeoff_typeisnotrequiredanymore_21 >> if_log_ifthetimeoff_typeisnotrequiredanymore_21_blank_22
        if_log_ifthetimeoff_typeisnotrequiredanymore_21_blank_22 >> rail.Label(
            'Yes') >> get_balance_summary_for_account_23 >> trigger_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_024 \
            >> append_timeoff_dag_run_024 >> foreach_declare_list_3_20_end
        foreach_declare_list_3_20 >> foreach_declare_list_3_20_end >> get_timeoff_child_dag_ids_20 >> wait_for_all_timeoff_dag_runs_20 >> foreach_d_25
        if_log_ifthetimeoff_typeisnotrequiredanymore_21_blank_22 >> rail.Label(
            'No') >> foreach_d_25
        foreach_d_25 >> if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_26
        if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_26 >> rail.Label(
            'Yes') >> log_checkifthetimeoffisalreadyassigned_27 >> get_default_time_off_type_policy_schedule_for_user_29 >> catch_121_121_30 \
            >> log_timeoff_policy_31 >> if_log_timeoff_policy_31_present_32
        if_log_timeoff_policy_31_present_32 >> rail.Label(
            'Yes') >> log_reset_balancefrompolicy_33 >> parse_json_34 >> log_reset_balance_35 >> log_defaultresetamount_36 >> declare_variable_37 \
            >> if_name_downcase_equals_to_sickleave_38
        if_name_downcase_equals_to_sickleave_38 >> rail.Label(
            'Yes') >> log_reset_balanceforparttime_39 >> update_variable_40 >> if_log_checkifthetimeoffisalreadyassigned_27_blank_41
        if_name_downcase_equals_to_sickleave_38 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_27_blank_41
        if_log_timeoff_policy_31_present_32 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_27_blank_41
        if_log_checkifthetimeoffisalreadyassigned_27_blank_41 >> rail.Label(
            'Yes') >> accumulate_list_items_42 >> if_log_timeoff_policy_31_present_43
        if_log_timeoff_policy_31_present_43 >> rail.Label(
            'Yes') >> if_name_downcase_equals_to_sickleave_44
        if_name_downcase_equals_to_sickleave_44 >> rail.Label(
            'Yes') >> log_lookupbalancebasedonmonthofhire_45 >> if_log_lookupbalancebasedonmonthofhire_45_present_46
        if_name_downcase_equals_to_sickleave_44 >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_59 >> if_log_checkifthetimeoffisalreadyassigned_27_present_60
        if_log_lookupbalancebasedonmonthofhire_45_present_46 >> rail.Label(
            'Yes') >> log_initial_balancefrompolicy_47 >> parse_json_48 >> log_initial_balance_49 >> log_valuefromdefault_50 \
            >> log_valuetobe_gsubbed_51 >> log_timeoff_policy_52 >> log_timeoff_policy_53 >> put_user_time_off_account_policy_set_schedule_54 \
            >> if_log_checkifthetimeoffisalreadyassigned_27_present_60
        if_log_lookupbalancebasedonmonthofhire_45_present_46 >> rail.Label(
            'No') >> log_timeoff_policy_56 >> put_user_time_off_account_policy_set_schedule_57 >> if_log_checkifthetimeoffisalreadyassigned_27_present_60

        if_log_timeoff_policy_31_present_43 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_27_present_60
        if_log_checkifthetimeoffisalreadyassigned_27_blank_41 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_27_present_60
        if_log_checkifthetimeoffisalreadyassigned_27_present_60 >> rail.Label(
            'Yes') >> if_name_downcase_equals_to_sickleave_61
        if_name_downcase_equals_to_sickleave_61 >> rail.Label(
            'Yes') >> log_lookupbalancebasedonmonthofhire_62 >> if_log_lookupbalancebasedonmonthofhire_62_present_63
        if_log_lookupbalancebasedonmonthofhire_62_present_63 >> rail.Label(
            'Yes') >> get_default_time_off_type_policy_schedule_for_user_65 >> log_timeoff_policy_67 >> if_log_timeoff_policy_67_present_68

        if_log_timeoff_policy_67_present_68 >> rail.Label(
            'Yes') >> log_initial_balancefrompolicy_69 >> parse_json_70 >> log_initial_balance_71 >> log_valuefromdefault_72 \
            >> log_valuetobe_gsubbed_73 >> declare_list_74 >> declare_list_75 >> log_existing_policy_76 >> if_log_existing_policy_76_present_77
        if_log_existing_policy_76_present_77 >> rail.Label(
            'Yes') >> parse_json_78 >> foreach_document_79 >> foreach_foreach_document_79_80 >> log_effectivedateforcomparison_81 \
            >> if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_82
        if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_82 >> rail.Label(
            'Yes') >> insert_to_list_83 >> foreach_foreach_document_79_80_end
        if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_82 >> rail.Label(
            'No') >> foreach_foreach_document_79_80_end
        foreach_foreach_document_79_80 >> foreach_foreach_document_79_80_end >> foreach_document_79_end

        foreach_document_79 >> foreach_document_79_end >> get_defaultpolicyfromgloballevel_84 >> foreach_response_85 >> log_policyset_86 \
            >> if_foreach_response_85_indexforeach_meta_equals_to_0_87
        if_log_existing_policy_76_present_77 >> rail.Label(
            'No') >> get_defaultpolicyfromgloballevel_84
        if_foreach_response_85_indexforeach_meta_equals_to_0_87 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_rehire_date_88 >> insert_to_list_89 >> foreach_response_85_end
        if_foreach_response_85_indexforeach_meta_equals_to_0_87 >> rail.Label(
            'No') >> log_required_effective_date_91 >> invoke_custom_ruby_code_required_effective_date_92 >> insert_to_list_93 >> foreach_response_85_end

        foreach_response_85 >> foreach_response_85_end >> log_existing_timeoff_policies_94 >> log_new_timeoff_policies_95 \
            >> log_new_timeoff_policies_96 >> if_log_existing_timeoff_policies_94_present_97
        if_log_existing_timeoff_policies_94_present_97 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_98 >> if_log_existing_timeoff_policies_94_blank_99
        if_log_existing_timeoff_policies_94_present_97 >> rail.Label(
            'No') >> if_log_existing_timeoff_policies_94_blank_99
        if_log_existing_timeoff_policies_94_blank_99 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_100 >> if_name_downcase_not_equals_to_sickleave_101
        if_log_existing_timeoff_policies_94_blank_99 >> rail.Label(
            'No') >> if_name_downcase_not_equals_to_sickleave_101

        if_log_timeoff_policy_67_present_68 >> rail.Label(
            'No') >> if_name_downcase_not_equals_to_sickleave_101
        if_log_lookupbalancebasedonmonthofhire_62_present_63 >> rail.Label(
            'No') >> if_name_downcase_not_equals_to_sickleave_101
        if_name_downcase_equals_to_sickleave_61 >> rail.Label(
            'No') >> if_name_downcase_not_equals_to_sickleave_101

        if_name_downcase_not_equals_to_sickleave_101 >> rail.Label(
            'Yes') >> get_defaultpolicyfromgloballevel_102 >> log_timeoff_policy_103 >> if_log_timeoff_policy_103_present_104
        if_log_timeoff_policy_103_present_104 >> rail.Label(
            'Yes') >> declare_list_105 >> log_existing_policy_106 >> if_log_existing_policy_106_present_107
        if_log_existing_policy_106_present_107 >> rail.Label(
            'Yes') >> parse_json_108 >> foreach_document_109 >> foreach_foreach_document_109_110 >> log_effectivedateforcomparison_111 \
            >> if_effective_date_for_comparison_less_than_rehire_date_112
        if_effective_date_for_comparison_less_than_rehire_date_112 >> rail.Label(
            'Yes') >> insert_to_list_113 >> foreach_foreach_response_110_end
        if_effective_date_for_comparison_less_than_rehire_date_112 >> rail.Label(
            'No') >> foreach_foreach_response_110_end
        foreach_foreach_document_109_110 >> foreach_foreach_response_110_end >> foreach_response_109_end
        foreach_document_109 >> foreach_response_109_end >> foreach_response_114
        if_log_existing_policy_106_present_107 >> rail.Label(
            'No') >> foreach_response_114 >> log_required_effective_date_115 >> invoke_custom_ruby_code_required_effective_date_116 \
            >> insert_to_list_117 >> foreach_response_114_end
        foreach_response_114 >> foreach_response_114_end >> log_timeoff_policies_118

        if_log_timeoff_policy_103_present_104 >> rail.Label(
            'No') >> log_timeoff_policies_118 >> if_log_timeoff_policies_118_present_119
        if_log_timeoff_policies_118_present_119 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_120 >> catch_error_121
        if_log_timeoff_policies_118_present_119 >> rail.Label('No') >> catch_error_121

        if_name_downcase_not_equals_to_sickleave_101 >> rail.Label(
            'No') >> catch_error_121
        if_log_checkifthetimeoffisalreadyassigned_27_present_60 >> rail.Label(
            'No') >> catch_error_121
        if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_26 >> rail.Label(
            'No') >> catch_error_121
        foreach_d_25 >> foreach_d_25_end >> catch_error_121
        if_log_12_present_17 >> rail.Label('No') >> catch_error_121
        if_first_displaytext_present_10 >> rail.Label(
            'No') >> catch_error_121

    return dag


rail.for_each_instance(create_dag)
