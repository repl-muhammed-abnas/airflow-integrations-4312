from datetime import datetime, timedelta
from pendulum import now
import json
from airflow.models import Variable
import rail
from fujifilmdbtl.user_import.utils import python_callable
from fujifilmdbtl.user_import.mapper.fujifilmdbtl_timeoff_balance_mapper import fdt_timeoff_balance_mapper


null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_child_add_remove_timeoff_type_for_rehire_termination_{config.instance}',
        description=f'FDT Child Workflow to add/remove timeoff type for Rehire - Termination{config.instance}',
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
            no_task='log_tenure_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_tenure_3',
            end_task='catch_error_143',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_tenure_3 = rail.PythonOperator(
            task_id='log_tenure_3',
            python_callable=lambda dag_run: float((datetime.strptime(dag_run.conf['enddate'], "%d/%m/%Y"
            ) - datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).days / 365)
        )

        declare_list_4 = rail.SetVariableOperator(
            task_id='declare_list_4',
            append=False,
            name='assigned_timeoff_types',
            value=[]
        )

        invoke_custom_ruby_code_todaysdate_5 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_todaysdate_5',
            python_callable=lambda: python_callable.get_split_date(
                now())
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
            items=lambda: rail.result('get_user_time_off_type_policy_summary_6')[
                'policiesByTimeOffType'],
            start_task='if_foreach_d_7_istimeoffallowedagainstthistimeofftype_is_true_8',
            end_task='foreach_d_7_end'
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
                "policyset": "{{result('foreach_d_7').policySetSchedule }}"
            }
        )

        foreach_d_7_end = rail.EmptyOperator(
            task_id='foreach_d_7_end',
        )

        _adhoc_http_action_10 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_10',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data=None
        )

        if_first_displaytext_present_11 = rail.IfOperator(
            task_id='if_first_displaytext_present_11',
            test='''{{result('_adhoc_http_action_10')[0].displayText | is_truthy }}''',
            yes_task="fdt_timeoff_balance_mapper_search_entries_12",
            no_task="catch_error_143",
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
            end_task='foreach_fdt_timeoff_balance_mapper_search_entries_12_14_end'
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

        def get_timeoff_type_uris():
            timeoff_entries = rail.get_dag_run_var('timeoff uris to assign')
            return [item['uri'] for item in timeoff_entries]

        log_final_set_timeoff_uris_17 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_17',
            python_callable=get_timeoff_type_uris
        )

        if_log_12_present_18 = rail.IfOperator(
            task_id='if_log_12_present_18',
            test='''{{ result('log_final_set_timeoff_uris_17') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_19",
            no_task="catch_error_143",
        )

        put_time_off_type_assignments_for_user_19 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_19',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUris": "{{ result('log_final_set_timeoff_uris_17') }}"
            }
        )

        get_user_time_off_type_policy_summary_20 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_20',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
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
            items=lambda: rail.get_dag_run_var('assigned_timeoff_types'),
            start_task='log_ifthetimeoff_typeisnotrequiredanymore_22',
            end_task='foreach_declare_list_4_21_end'
        )

        log_ifthetimeoff_typeisnotrequiredanymore_22 = rail.PythonOperator(
            task_id='log_ifthetimeoff_typeisnotrequiredanymore_22',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('accumulate_list_items_16'), 'uri', rail.result(
                'foreach_declare_list_4_21')['uri']) if rail.result('accumulate_list_items_16')[0]['name'] else ""
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
                    "year": "{{ result('invoke_custom_ruby_code_todaysdate_5').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_todaysdate_5').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_todaysdate_5').day }}"
                }
            }
        )

        trigger_dag_run_fujifilmdbtl_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_25 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_fujifilmdbtl_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_25',
            retries=0,
            trigger_dag_id=f'fujifilmdbtl_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('foreach_declare_list_4_21').uri }}",
                "policyset": lambda: rail.result('foreach_declare_list_4_21')['policyset'],
                "enddate": now().strftime("%d/%m/%Y"),
                "newschedulebalance": "{{ result('get_balance_summary_for_account_24').timeRemaining }}"
            }
        )

        # Append the triggered DAG run to the collection variable
        append_timeoff_dag_run_25 = rail.SetVariableOperator(
            task_id='append_timeoff_dag_run_25',
            name='timeoff_dag_runs_21',
            append=True,
            value='{{ result("trigger_dag_run_fujifilmdbtl_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_25") }}'
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
            yes_task="log_checkifthetimeoffisalreadyassigned_28",
            no_task="foreach_d_26_end",
        )

        log_checkifthetimeoffisalreadyassigned_28 = rail.PythonOperator(
            task_id='log_checkifthetimeoffisalreadyassigned_28',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('assigned_timeoff_types'), 'uri', rail.result(
                'foreach_d_26')['timeOffType']['uri']) if rail.get_dag_run_var('assigned_timeoff_types')[0]['name'] else ""
        )

        if_log_checkifthetimeoffisalreadyassigned_28_blank_29 = rail.IfOperator(
            task_id='if_log_checkifthetimeoffisalreadyassigned_28_blank_29',
            test='''{{ result('log_checkifthetimeoffisalreadyassigned_28') | is_falsy }}''',
            yes_task="accumulate_list_items_30",
            no_task="if_log_checkifthetimeoffisalreadyassigned_28_present_60",
        )

        accumulate_list_items_30 = rail.SetVariableOperator(
            task_id='accumulate_list_items_30',
            name='assigned_timeoff_types',
            append=True,
            value={
                "timeofftype": "{{ result('foreach_d_26').timeOffType.name }}"
            }
        )

        get_default_time_off_type_policy_schedule_for_user_32 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_32',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
                }
            }
        )

        log_timeoff_policy_34 = rail.PythonOperator(
            task_id='log_timeoff_policy_34',
            python_callable=lambda:  json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_32')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"') if rail.result('get_default_time_off_type_policy_schedule_for_user_32')[0]['policySet'] else ""
        )

        log_reset_balancefrompolicy_35 = rail.PythonOperator(
            task_id='log_reset_balancefrompolicy_35',
            python_callable=lambda:  json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_type_policy_schedule_for_user_32')[
                                                0]['policySet']['timeOffBalanceEventScripts'], "script.name",  "Yearly Reset", 'additionalParameters')).replace("[[", "[").replace("]]", "]")
        )

        parse_json_36 = rail.PythonOperator(
            task_id='parse_json_36',
            python_callable=lambda: json.loads(
                rail.result('log_reset_balancefrompolicy_35'))
        )

        log_reset_balance_37 = rail.PythonOperator(
            task_id='log_reset_balance_37',
            python_callable=lambda: str(float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_36'), 'keyUri', 'urn:replicon:script-key:parameter:reset-balance-amount', 'value.number')))
        )

        log_defaultresetamount_38 = rail.PythonOperator(
            task_id='log_defaultresetamount_38',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                                                "value": {"number": rail.result('log_reset_balance_37')}})
        )

        declare_variable_39 = rail.SetVariableOperator(
            task_id='declare_variable_39',
            append=False,
            name='sick_reset_amount',
            value=lambda: rail.result('log_defaultresetamount_38')
        )

        if_name_downcase_equals_to_sickleave_40 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_40',
            test=lambda dag_run: (rail.result('foreach_d_26')['timeOffType']['name']).lower(
            ) == 'sick leave' and dag_run.conf['ftpt'] == 'p',
            yes_task="log_reset_balanceforparttime_41",
            no_task="if_log_13_present_43",
        )

        log_reset_balanceforparttime_41 = rail.PythonOperator(
            task_id='log_reset_balanceforparttime_41',
            python_callable=lambda:  str(
                float(rail.result('log_reset_balance_37')) / 2)
        )

        update_variable_42 = rail.SetVariableOperator(
            task_id='update_variable_42',
            append=False,
            name='{{ result("declare_variable_39").name }}',
            value=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                                      "value": {"number": rail.result('log_reset_balanceforparttime_41')}})
        )

        if_log_13_present_43 = rail.IfOperator(
            task_id='if_log_13_present_43',
            test='''{{ result('log_timeoff_policy_34') | is_truthy }}''',
            yes_task="if_name_downcase_equals_to_sickleave_44",
            no_task="if_log_checkifthetimeoffisalreadyassigned_28_present_60",
        )

        if_name_downcase_equals_to_sickleave_44 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_44',
            test=lambda: (rail.result('foreach_d_26')['timeOffType']['name']).lower() == 'sick leave' or (
                rail.result('foreach_d_26')['timeOffType']['name']).lower() == 'floating holiday',
            yes_task="log_lookupbalancebasedonmonthofhire_45",
            no_task="put_user_time_off_account_policy_set_schedule_59",
        )

        log_lookupbalancebasedonmonthofhire_45 = rail.PythonOperator(
            task_id='log_lookupbalancebasedonmonthofhire_45',
            python_callable=lambda dag_run:  (list(filter(lambda x: x["type"] == (rail.result('foreach_d_26')['timeOffType']['name']).lower(
            ) and x["monthofhire"] == dag_run.conf['startdatemonth'] and x["ftpt"] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper)))[0]['balance']
        )

        if_log_lookupbalancebasedonmonthofhire_45_present_46 = rail.IfOperator(
            task_id='if_log_lookupbalancebasedonmonthofhire_45_present_46',
            test='''{{ result('log_lookupbalancebasedonmonthofhire_45') | is_truthy }}''',
            yes_task="log_initial_balancefrompolicy_47",
            no_task="log_timeoff_policy_56",
        )

        log_initial_balancefrompolicy_47 = rail.PythonOperator(
            task_id='log_initial_balancefrompolicy_47',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_type_policy_schedule_for_user_32')[
                                               0]['policySet']['timeOffBalanceEventScripts'], "script.name",  "Starting Balance Set To", 'additionalParameters')).replace("[[", "[").replace("]]", "]")
        )

        parse_json_48 = rail.PythonOperator(
            task_id='parse_json_48',
            python_callable=lambda: json.loads(
                rail.result('log_initial_balancefrompolicy_47'))
        )

        log_initial_balance_49 = rail.PythonOperator(
            task_id='log_initial_balance_49',
            python_callable=lambda: str(float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_48'), 'keyUri', "urn:replicon:script-key:parameter:amount", 'value.number')))
        )

        log_valuefromdefault_50 = rail.PythonOperator(
            task_id='log_valuefromdefault_50',
            python_callable=lambda:  json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                 "value": {"number": rail.result('log_initial_balance_49')}})
        )

        log_valuetobe_gsubbed_51 = rail.PythonOperator(
            task_id='log_valuetobe_gsubbed_51',
            python_callable=lambda:  json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                 "value": {"number": rail.result('log_lookupbalancebasedonmonthofhire_45')}})
        )

        log_timeoff_policy_52 = rail.PythonOperator(
            task_id='log_timeoff_policy_52',
            python_callable=lambda:  json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_32')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace(rail.result('log_valuefromdefault_50'), rail.result('log_valuetobe_gsubbed_51'))
        )

        log_timeoff_policy_53 = rail.PythonOperator(
            task_id='log_timeoff_policy_53',
            python_callable=lambda:  rail.result('log_timeoff_policy_52').replace(rail.result('log_defaultresetamount_38'), rail.get_dag_run_var(
                'sick_reset_amount')) if (rail.result('foreach_d_26')['timeOffType']['name']).lower() == 'sick leave' else rail.result('log_timeoff_policy_52')
        )

        put_user_time_off_account_policy_set_schedule_54 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_54',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
                },
                "policySetScheduleEntries": "{{ result('log_timeoff_policy_53') }}"
            }
        )

        log_timeoff_policy_56 = rail.PythonOperator(
            task_id='log_timeoff_policy_56',
            python_callable=lambda: rail.result('log_timeoff_policy_34').replace(rail.result('log_defaultresetamount_38'), rail.get_dag_run_var(
                'sick_reset_amount')) if (rail.result('foreach_d_26')['timeOffType']['name']).lower() == 'sick leave' else rail.result('log_timeoff_policy_34')
        )

        put_user_time_off_account_policy_set_schedule_57 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_57',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
                },
                "policySetScheduleEntries": "{{ result('log_timeoff_policy_34') }}"
            }
        )

        put_user_time_off_account_policy_set_schedule_59 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_59',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
                },
                "policySetScheduleEntries": "{{ result('log_timeoff_policy_34') }}"
            }
        )

        if_log_checkifthetimeoffisalreadyassigned_28_present_60 = rail.IfOperator(
            task_id='if_log_checkifthetimeoffisalreadyassigned_28_present_60',
            test='''{{ result('log_checkifthetimeoffisalreadyassigned_28') | is_truthy }}''',
            yes_task="if_name_downcase_equals_to_sickleave_61",
            no_task="foreach_d_26_end",
        )

        if_name_downcase_equals_to_sickleave_61 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_61',
            test=lambda: (rail.result('foreach_d_26')['timeOffType']['name']).lower() == 'sick leave' or (
                rail.result('foreach_d_26')['timeOffType']['name']).lower() == 'floating holiday',
            yes_task="log_lookupbalancebasedonmonthofhire_62",
            no_task="if_name_downcase_not_equals_to_sickleave_109",
        )

        log_lookupbalancebasedonmonthofhire_62 = rail.PythonOperator(
            task_id='log_lookupbalancebasedonmonthofhire_62',
            python_callable=lambda dag_run:  (list(filter(lambda x: x["type"] == (rail.result('foreach_d_26')['timeOffType']['name']).lower(
            ) and x["monthofhire"] == dag_run.conf['startdatemonth'] and x["ftpt"] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper)))[0]['balance']
        )

        if_log_lookupbalancebasedonmonthofhire_62_present_63 = rail.IfOperator(
            task_id='if_log_lookupbalancebasedonmonthofhire_62_present_63',
            test='''{{ result('log_lookupbalancebasedonmonthofhire_62') | is_truthy }}''',
            yes_task="get_default_time_off_type_policy_schedule_for_user_65",
            no_task="if_name_downcase_not_equals_to_sickleave_109",
        )

        get_default_time_off_type_policy_schedule_for_user_65 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_65',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
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
            no_task="if_name_downcase_not_equals_to_sickleave_109",
        )

        log_initial_balancefrompolicy_69 = rail.PythonOperator(
            task_id='log_initial_balancefrompolicy_69',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_type_policy_schedule_for_user_65')[
                                               0]['policySet']['timeOffBalanceEventScripts'], "script.name",  "Starting Balance Set To", 'additionalParameters')).replace("[[", "[").replace("]]", "]")
        )

        parse_json_70 = rail.PythonOperator(
            task_id='parse_json_70',
            python_callable=lambda: json.loads(
                rail.result('log_initial_balancefrompolicy_69'))
        )

        log_initial_balance_71 = rail.PythonOperator(
            task_id='log_initial_balance_71',
            python_callable=lambda: str(float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_70'), 'keyUri', 'urn:replicon:script-key:parameter:reset-balance-amount', 'value.number')))
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
            python_callable=lambda:  json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_type_policy_summary_20')[
                                                'policiesByTimeOffType'], 'timeOffType.uri', rail.result('foreach_d_26')['timeOffType']['uri'], 'policySetSchedule'))
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
            test=lambda dag_run: datetime.strptime(rail.result('log_effectivedateforcomparison_81'), "%d/%m/%Y").isoformat(
            ) < datetime.strptime(dag_run.conf['rehiredate'], "%d/%m/%Y").isoformat(),
            yes_task="insert_to_list_83",
            no_task="foreach_foreach_document_79_80_end",
        )

        insert_to_list_83 = rail.SetVariableOperator(
            task_id='insert_to_list_83',
            append=True,
            name='{{ result("declare_list_74").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('foreach_foreach_document_79_80').effectiveDate.day }}",
                    "month": "{{ result('foreach_foreach_document_79_80').effectiveDate.month }}",
                    "year": "{{ result('foreach_foreach_document_79_80').effectiveDate.year }}"
                },
                "description": "{{ result('foreach_foreach_document_79_80').description }}",
                "policySet": "{{ result('foreach_foreach_document_79_80').policySet')}}"
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
            data={
                "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
            }
        )

        declare_list_85 = rail.SetVariableOperator(
            task_id='declare_list_85',
            append=False,
            name='count_of_policy',
            value=[]
        )

        foreach_response_86 = rail.ForEachOperator(
            task_id='foreach_response_86',
            items=lambda: rail.result('get_defaultpolicyfromgloballevel_84'),
            start_task='if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_87',
            end_task='foreach_response_86_end'
        )

        if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_87 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_87',
            test=lambda: float(rail.result('foreach_response_86')['startOffset']['offsetValue']) == float(rail.result(
                'log_tenure_3')) or float(rail.result('foreach_response_86')['startOffset']['offsetValue']) > float(rail.result('log_tenure_3')),
            yes_task="insert_to_list_88",
            no_task="foreach_response_86_end",
        )

        insert_to_list_88 = rail.SetVariableOperator(
            task_id='insert_to_list_88',
            append=True,
            name='{{ result("declare_list_85").name }}',
            value={
                "count": "{{ result('foreach_response_86').startOffset.offsetValue }}",
                "policy": "{{ result('foreach_response_86').policySet }}"
            }
        )

        foreach_response_86_end = rail.EmptyOperator(
            task_id='foreach_response_86_end',
        )

        foreach_declare_list_85_89 = rail.ForEachOperator(
            task_id='foreach_declare_list_85_89',
            items=lambda: rail.get_dag_run_var('count_of_policy'),
            start_task='log_policyset_90',
            end_task='foreach_declare_list_85_89_end'
        )

        log_policyset_90 = rail.PythonOperator(
            task_id='log_policyset_90',
            python_callable=lambda: rail.result(
                'foreach_declare_list_85_89')['policy']
        )

        if_foreach_declare_list_85_89_indexforeach_meta_equals_to_0_91 = rail.IfOperator(
            task_id='if_foreach_declare_list_85_89_indexforeach_meta_equals_to_0_91',
            test=lambda: rail.result('foreach_declare_list_85_89') == rail.get_dag_run_var(
                'count_of_policy')[0],
            yes_task="invoke_custom_ruby_code_rehire_date_92",
            no_task="log_required_effective_date_95",
        )

        invoke_custom_ruby_code_rehire_date_92 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_rehire_date_92',
            python_callable=lambda dag_run:  python_callable.get_split_date(
                dag_run.conf['rehiredate'], "%d/%m/%Y")
        )

        insert_to_list_93 = rail.SetVariableOperator(
            task_id='insert_to_list_93',
            append=True,
            name='{{ result("declare_list_75").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_rehire_date_92').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_rehire_date_92').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_rehire_date_92').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_rehire_date_92').day }}/{{ result('invoke_custom_ruby_code_rehire_date_92').month }}/{{ result('invoke_custom_ruby_code_rehire_date_92').year }}",
                "policySet": "{{ result('foreach_declare_list_85_89').policy }}"
            }
        )

        log_required_effective_date_95 = rail.PythonOperator(
            task_id='log_required_effective_date_95',
            python_callable=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y") + timedelta(days=rail.result('foreach_declare_list_85_89')['count'] * 365)
        )

        invoke_custom_ruby_code_required_effective_date_96 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_required_effective_date_96',
            python_callable=lambda: python_callable.get_split_date(
                rail.result('log_required_effective_date_95'))
        )

        insert_to_list_97 = rail.SetVariableOperator(
            task_id='insert_to_list_97',
            append=True,
            name='{{ result("declare_list_75").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_required_effective_date_96').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_required_effective_date_96').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_required_effective_date_96').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_required_effective_date_96').day }}/{{ result('invoke_custom_ruby_code_required_effective_date_96').month }}/{{ result('invoke_custom_ruby_code_required_effective_date_96').year }}",
                "policySet": "{{ result('foreach_declare_list_85_89').policy }}"
            }
        )

        foreach_declare_list_85_89_end = rail.EmptyOperator(
            task_id='foreach_declare_list_85_89_end',
        )

        if_declare_list_75_list_items_equals_to_0_98 = rail.IfOperator(
            task_id='if_declare_list_75_list_items_equals_to_0_98',
            test=lambda: bool(
                len(rail.get_dag_run_var('newpolicyschedules')) == 0),
            yes_task="foreach_response_99",
            no_task="log_existing_timeoff_policies_104",
        )

        foreach_response_99 = rail.ForEachOperator(
            task_id='foreach_response_99',
            items=lambda: rail.result('get_defaultpolicyfromgloballevel_84'),
            start_task='if_foreach_response_99_indexforeach_meta_equals_to_dataforeachforeach_response_99sizeforeach_meta1_100',
            end_task='foreach_response_99_end'
        )

        last_item_in_get_defaultpolicyfromgloballevel = rail.PythonOperator(
            task_id='last_item_in_get_defaultpolicyfromgloballevel',
            python_callable=lambda: rail.result('get_defaultpolicyfromgloballevel_84')[
                len(rail.result('get_defaultpolicyfromgloballevel_84'))-1]
        )

        if_foreach_response_99_indexforeach_meta_equals_to_dataforeachforeach_response_99sizeforeach_meta1_100 = rail.IfOperator(
            task_id='if_foreach_response_99_indexforeach_meta_equals_to_dataforeachforeach_response_99sizeforeach_meta1_100',
            test=lambda: bool(rail.result('foreach_response_99') == rail.result(
                'last_item_in_get_defaultpolicyfromgloballevel')),
            yes_task="log_policyset_101",
            no_task="foreach_response_99_end",
        )

        log_policyset_101 = rail.PythonOperator(
            task_id='log_policyset_101',
            python_callable=lambda: rail.result(
                'foreach_response_86')['policySet']
        )

        invoke_custom_ruby_code_rehire_date_102 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_rehire_date_102',
            python_callable=lambda dag_run:  python_callable.get_split_date(
                dag_run.conf['rehiredate'], "%d/%m/%Y")
        )

        insert_to_list_103 = rail.SetVariableOperator(
            task_id='insert_to_list_103',
            append=True,
            name='{{ result("declare_list_75").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_rehire_date_102').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_rehire_date_102').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_rehire_date_102').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_rehire_date_102').day }}/{{ result('invoke_custom_ruby_code_rehire_date_102').month }}/{{ result('invoke_custom_ruby_code_rehire_date_102').year }}",
                "policySet": "{{result('log_policyset_101')}}"
            }
        )

        foreach_response_99_end = rail.EmptyOperator(
            task_id='foreach_response_99_end',
        )

        log_existing_timeoff_policies_104 = rail.PythonOperator(
            task_id='log_existing_timeoff_policies_104',
            python_callable=lambda: json.dumps(rail.get_dag_run_var('oldpolicyschedules')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace("}}]}}]", "}}]}}") if rail.get_dag_run_var('oldpolicyschedules')[0]['policySet'] else ""
        )

        log_new_timeoff_policies_105 = rail.PythonOperator(
            task_id='log_new_timeoff_policies_105',
            python_callable=lambda: json.dumps(rail.get_dag_run_var('newpolicyschedule')).replace('null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"').replace(rail.result('log_valuefromdefault_72'), rail.result('log_valuetobe_gsubbed_73')).replace(
                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace("}}]}]}]", "}}]}}]").replace('[{"effectiveDate', '{"effectiveDate') if rail.get_dag_run_var('newpolicyschedules')[0]['policySet'] else ""
        )

        log_new_policies_106 = rail.PythonOperator(
            task_id='log_new_policies_106',
            python_callable=lambda: rail.result('log_new_timeoff_policies_105').replace(rail.result('log_valuefromdefault_72'), rail.result('log_valuetobe_gsubbed_73')) if (
                rail.result('foreach_d_26')['timeOffType']['name']).lower() == 'sick leave' else rail.result('log_new_timeoff_policies_105')
        )

        if_log_existing_timeoff_policies_104_present_107 = rail.IfOperator(
            task_id='if_log_existing_timeoff_policies_104_present_107',
            test='''{{ result('log_existing_timeoff_policies_104') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_108",
            no_task="if_name_downcase_not_equals_to_sickleave_109",
        )

        put_user_time_off_account_policy_set_schedule_108 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_108',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
                },
                "policySetScheduleEntries": ["{{ result('log_existing_timeoff_policies_104') }}", "{{ result('log_new_policies_106') }}"]
            }
        )

        if_name_downcase_not_equals_to_sickleave_109 = rail.IfOperator(
            task_id='if_name_downcase_not_equals_to_sickleave_109',
            test=lambda: bool((rail.result('foreach_d_26')['timeOffType']['name']).lower() == 'sick leave' and (
                rail.result('foreach_d_26')['timeOffType']['name']).lower() == 'floating holiday'),
            yes_task="get_defaultpolicyfromgloballevel_110",
            no_task="foreach_d_26_end",
        )

        get_defaultpolicyfromgloballevel_110 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_110',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
            }
        )

        log_timeoff_policy_111 = rail.PythonOperator(
            task_id='log_timeoff_policy_111',
            python_callable=lambda: rail.result('get_defaultpolicyfromgloballevel_110')[
                0]['policySet'] if rail.result('get_defaultpolicyfromgloballevel_110')[0]['policySet'] else ""
        )

        if_log_timeoff_policy_111_present_112 = rail.IfOperator(
            task_id='if_log_timeoff_policy_111_present_112',
            test='''{{ result('log_timeoff_policy_111') | is_truthy }}''',
            yes_task="declare_list_113",
            no_task="log_timeoff_policies_140",
        )

        declare_list_113 = rail.SetVariableOperator(
            task_id='declare_list_113',
            append=False,
            name='policyschedules',
            value=[]
        )

        log_existing_policy_114 = rail.PythonOperator(
            task_id='log_existing_policy_114',
            python_callable=lambda:  json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_type_policy_summary_20')[
                                                "policiesByTimeOffType"], "timeOffType.uri", rail.result('foreach_d_26')['timeOffType']['uri'], 'policySetSchedule', ""))
        )

        if_log_existing_policy_114_present_115 = rail.IfOperator(
            task_id='if_log_existing_policy_114_present_115',
            test='''{{ result('log_existing_policy_114') | is_truthy }}''',
            yes_task="parse_json_116",
            no_task="declare_list_122",
        )

        parse_json_116 = rail.PythonOperator(
            task_id='parse_json_116',
            python_callable=lambda: json.loads(
                rail.result('log_existing_policy_114'))
        )

        foreach_document_117 = rail.ForEachOperator(
            task_id='foreach_document_117',
            items=lambda: rail.result('parse_json_116'),
            start_task='foreach_foreach_document_117_118',
            end_task='foreach_document_117_end'
        )

        foreach_foreach_document_117_118 = rail.ForEachOperator(
            task_id='foreach_foreach_document_117_118',
            items=lambda: rail.result('foreach_document_117'),
            start_task='log_effectivedateforcomparison_119',
            end_task='foreach_foreach_document_117_118_end'
        )

        log_effectivedateforcomparison_119 = rail.PythonOperator(
            task_id='log_effectivedateforcomparison_119',
            python_callable=lambda:  "{{ result('foreach_foreach_document_117_118').effectiveDate.day }}/{{ result('foreach_foreach_document_117_118').effectiveDate.month }}/{{ result('foreach_foreach_document_117_118').effectiveDate.year }}"
        )

        if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_120 = rail.IfOperator(
            task_id='if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_120',
            test=lambda dag_run: datetime.strptime(rail.result('log_effectivedateforcomparison_119'), "%d/%m/%Y").isoformat(
            ) < datetime.strptime(dag_run.conf['rehiredate'], "%d/%m/%Y").isoformat(),
            yes_task="insert_to_list_121",
            no_task="foreach_foreach_document_117_118_end",
        )

        insert_to_list_121 = rail.SetVariableOperator(
            task_id='insert_to_list_121',
            append=True,
            name='{{ result("declare_list_113").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('foreach_foreach_document_117_118').effectiveDate.day }}",
                    "month": "{{ result('foreach_foreach_document_117_118').effectiveDate.month }}",
                    "year": "{{ result('foreach_foreach_document_117_118').effectiveDate.year }}"
                },
                "description": "{{ result('foreach_foreach_document_117_118').description }}",
                "policySet": "{{ result('foreach_foreach_document_117_118').policySet }}"
            }
        )

        foreach_foreach_document_117_118_end = rail.EmptyOperator(
            task_id='foreach_foreach_document_117_118_end',
        )

        foreach_document_117_end = rail.EmptyOperator(
            task_id='foreach_document_117_end',
        )

        declare_list_122 = rail.SetVariableOperator(
            task_id='declare_list_122',
            append=False,
            name='count_of_policy',
            value=[]
        )

        foreach_response_123 = rail.ForEachOperator(
            task_id='foreach_response_123',
            items=lambda: rail.result('get_defaultpolicyfromgloballevel_110'),
            start_task='if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_124',
            end_task='foreach_response_123_end'
        )

        if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_124 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_124',
            test=lambda: float(rail.result('foreach_response_123')[
                'startOffset']['offsetValue']) >= float(rail.result('log_tenure_3')),
            yes_task="insert_to_list_125",
            no_task="foreach_response_123_end",
        )

        insert_to_list_125 = rail.SetVariableOperator(
            task_id='insert_to_list_125',
            append=True,
            name='{{ result("declare_list_122").name }}',
            value={
                "count": "{{ result('foreach_response_123').startOffset.offsetValue }}",
                "policy": "{{ result('foreach_response_123').policySet'}}"
            }
        )

        accumulate_list_items_126 = rail.SetVariableOperator(
            task_id='accumulate_list_items_126',
            name='count_of_new_policies',
            append=True,
            value={
                "count": "{{ result('foreach_response_123').startOffset.offsetValue }}"
            }
        )

        foreach_response_123_end = rail.EmptyOperator(
            task_id='foreach_response_123_end',
        )

        foreach_declare_list_122_127 = rail.ForEachOperator(
            task_id='foreach_declare_list_122_127',
            items=lambda: rail.get_dag_run_var('count_of_policy'),
            start_task='if_foreach_declare_list_122_127_indexforeach_meta_equals_to_0_128',
            end_task='foreach_declare_list_122_127_end'
        )

        if_foreach_declare_list_122_127_indexforeach_meta_equals_to_0_128 = rail.IfOperator(
            task_id='if_foreach_declare_list_122_127_indexforeach_meta_equals_to_0_128',
            test=lambda: rail.result(
                'foreach_declare_list_122_127') == rail.get_dag_run_var('count_of_policy')[0],
            yes_task="invoke_custom_ruby_code_required_effective_date_129",
            no_task="log_required_effective_date_132",
        )

        invoke_custom_ruby_code_required_effective_date_129 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_required_effective_date_129',
            python_callable=lambda dag_run:  python_callable.get_split_date(
                dag_run.conf['rehiredate'], "%d/%m/%Y")
        )

        insert_to_list_130 = rail.SetVariableOperator(
            task_id='insert_to_list_130',
            append=True,
            name='{{ result("declare_list_113").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_required_effective_date_129').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_required_effective_date_129').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_required_effective_date_129').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_required_effective_date_129').day }}/{{ result('invoke_custom_ruby_code_required_effective_date_129').month }}/{{ result('invoke_custom_ruby_code_required_effective_date_129').year }}",
                "policySet": "{{ result('foreach_declare_list_122_127').policy}}"
            }
        )

        log_required_effective_date_132 = rail.PythonOperator(
            task_id='log_required_effective_date_132',
            python_callable=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y") + timedelta(days=rail.result('foreach_declare_list_122_127')['count'] * 365)
        )

        invoke_custom_ruby_code_required_effective_date_133 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_required_effective_date_133',
            python_callable=lambda:  python_callable.get_split_date(
                rail.result('log_required_effective_date_132'))
        )

        insert_to_list_134 = rail.SetVariableOperator(
            task_id='insert_to_list_134',
            append=True,
            name='{{ result("declare_list_113").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_required_effective_date_133').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_required_effective_date_133').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_required_effective_date_133').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_required_effective_date_133').day }}/{{ result('invoke_custom_ruby_code_required_effective_date_133').month }}/{{ result('invoke_custom_ruby_code_required_effective_date_133').year }}",
                "policySet": "{{ result('foreach_declare_list_122_127').policy}}"
            }
        )

        foreach_declare_list_122_127_end = rail.EmptyOperator(
            task_id='foreach_declare_list_122_127_end',
        )

        if_accumulate_list_items_126_list_items_equals_to_0_135 = rail.IfOperator(
            task_id='if_accumulate_list_items_126_list_items_equals_to_0_135',
            test=lambda: bool(
                len(rail.get_dag_run_var('count_of_new_policies')) == 0),
            yes_task="foreach_response_136",
            no_task="log_timeoff_policies_140",
        )

        foreach_response_136 = rail.ForEachOperator(
            task_id='foreach_response_136',
            items=lambda: rail.result('get_defaultpolicyfromgloballevel_110'),
            start_task='if_foreach_response_136_indexforeach_meta_equals_to_dataforeachforeach_response_136sizeforeach_meta1_137',
            end_task='foreach_response_136_end'
        )

        if_foreach_response_136_indexforeach_meta_equals_to_dataforeachforeach_response_136sizeforeach_meta1_137 = rail.IfOperator(
            task_id='if_foreach_response_136_indexforeach_meta_equals_to_dataforeachforeach_response_136sizeforeach_meta1_137',
            test=lambda: bool(rail.result('foreach_response_136') == rail.result(
                'foreach_response_136')[len(rail.result('foreach_response_136'))-1]),
            yes_task="invoke_custom_ruby_code_required_effective_date_138",
            no_task="foreach_response_136_end",
        )

        invoke_custom_ruby_code_required_effective_date_138 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_required_effective_date_138',
            python_callable=lambda dag_run:  python_callable.get_split_date(
                dag_run.conf['rehiredate'], "%d/%m/%Y")
        )

        insert_to_list_139 = rail.SetVariableOperator(
            task_id='insert_to_list_139',
            append=True,
            name='{{ result("declare_list_113").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('invoke_custom_ruby_code_required_effective_date_138').day }}",
                    "month": "{{ result('invoke_custom_ruby_code_required_effective_date_138').month }}",
                    "year": "{{ result('invoke_custom_ruby_code_required_effective_date_138').year }}"
                },
                "description": "Effective on {{ result('invoke_custom_ruby_code_required_effective_date_138').day }}/{{ result('invoke_custom_ruby_code_required_effective_date_138').month }}/{{ result('invoke_custom_ruby_code_required_effective_date_138').year }}",
                "policySet": "{{result('foreach_response_136').policySet }}"
            }
        )

        foreach_response_136_end = rail.EmptyOperator(
            task_id='foreach_response_136_end',
        )

        log_timeoff_policies_140 = rail.PythonOperator(
            task_id='log_timeoff_policies_140',
            python_callable=lambda: json.dumps(rail.get_dag_run_var('policyschedules')).replace('null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"').replace(
                '[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts').replace("}}]}]}]", "}}]}}]") if rail.get_dag_run_var('policyschedules')[0]['policySet'] else ""
        )

        if_log_timeoff_policies_140_present_141 = rail.IfOperator(
            task_id='if_log_timeoff_policies_140_present_141',
            test='''{{ result('log_timeoff_policies_140') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_142",
            no_task="foreach_d_26_end",
        )

        put_user_time_off_account_policy_set_schedule_142 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_142',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_26').timeOffType.uri }}"
                },
                "policySetScheduleEntries": "{{ result('log_timeoff_policies_140') }}"
            }
        )

        foreach_d_26_end = rail.EmptyOperator(
            task_id='foreach_d_26_end',
        )

        catch_error_143 = rail.EmptyOperator(
            task_id='catch_error_143',
            trigger_rule='one_failed',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error_143
        can_run_batch_task >> rail.Label('No') >> log_tenure_3
        log_tenure_3 >> declare_list_4 >> invoke_custom_ruby_code_todaysdate_5 >> get_user_time_off_type_policy_summary_6 \
            >> foreach_d_7 >> if_foreach_d_7_istimeoffallowedagainstthistimeofftype_is_true_8
        if_foreach_d_7_istimeoffallowedagainstthistimeofftype_is_true_8 >> rail.Label(
            'Yes') >> insert_to_list_9 >> foreach_d_7_end
        if_foreach_d_7_istimeoffallowedagainstthistimeofftype_is_true_8 >> rail.Label(
            'No') >> foreach_d_7_end
        foreach_d_7 >> foreach_d_7_end >> _adhoc_http_action_10 >> if_first_displaytext_present_11
        if_first_displaytext_present_11 >> rail.Label('Yes') >> fdt_timeoff_balance_mapper_search_entries_12 >> declare_list_13 \
            >> foreach_fdt_timeoff_balance_mapper_search_entries_12_14 >> insert_to_list_15 >> accumulate_list_items_16 >> foreach_fdt_timeoff_balance_mapper_search_entries_12_14_end
        foreach_fdt_timeoff_balance_mapper_search_entries_12_14 >> foreach_fdt_timeoff_balance_mapper_search_entries_12_14_end >> log_final_set_timeoff_uris_17 >> if_log_12_present_18
        if_log_12_present_18 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_19 >> get_user_time_off_type_policy_summary_20 >> init_timeoff_dag_runs_list_21 >> foreach_declare_list_4_21 >> log_ifthetimeoff_typeisnotrequiredanymore_22 >> if_log_ifthetimeoff_typeisnotrequiredanymore_22_blank_23
        if_log_ifthetimeoff_typeisnotrequiredanymore_22_blank_23 >> rail.Label(
            'Yes') >> get_balance_summary_for_account_24 >> trigger_dag_run_fujifilmdbtl_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_25 >> append_timeoff_dag_run_25 >> foreach_declare_list_4_21_end
        if_log_ifthetimeoff_typeisnotrequiredanymore_22_blank_23 >> rail.Label(
            'No') >> foreach_declare_list_4_21_end

        foreach_declare_list_4_21 >> foreach_declare_list_4_21_end >> get_timeoff_child_dag_ids_21 >> wait_for_all_timeoff_dag_runs_21 >> foreach_d_26 >> if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_27
        if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_27 >> rail.Label(
            'Yes') >> log_checkifthetimeoffisalreadyassigned_28 >> if_log_checkifthetimeoffisalreadyassigned_28_blank_29
        if_log_checkifthetimeoffisalreadyassigned_28_blank_29 >> rail.Label(
            'Yes') >> accumulate_list_items_30 >> get_default_time_off_type_policy_schedule_for_user_32 >> log_timeoff_policy_34 >> log_reset_balancefrompolicy_35 >> parse_json_36 >> log_reset_balance_37 >> log_defaultresetamount_38 >> declare_variable_39 >> if_name_downcase_equals_to_sickleave_40
        if_name_downcase_equals_to_sickleave_40 >> rail.Label(
            'Yes') >> log_reset_balanceforparttime_41 >> update_variable_42 >> if_log_13_present_43
        if_name_downcase_equals_to_sickleave_40 >> rail.Label(
            'No') >> if_log_13_present_43
        if_log_13_present_43 >> rail.Label(
            'Yes') >> if_name_downcase_equals_to_sickleave_44
        if_name_downcase_equals_to_sickleave_44 >> rail.Label(
            'Yes') >> log_lookupbalancebasedonmonthofhire_45 >> if_log_lookupbalancebasedonmonthofhire_45_present_46
        if_log_lookupbalancebasedonmonthofhire_45_present_46 >> rail.Label(
            'Yes') >> log_initial_balancefrompolicy_47 >> parse_json_48 >> log_initial_balance_49 >> log_valuefromdefault_50 >> log_valuetobe_gsubbed_51 >> log_timeoff_policy_52 >> log_timeoff_policy_53 >> put_user_time_off_account_policy_set_schedule_54 >> if_log_checkifthetimeoffisalreadyassigned_28_present_60
        if_log_lookupbalancebasedonmonthofhire_45_present_46 >> rail.Label(
            'No') >> log_timeoff_policy_56 >> put_user_time_off_account_policy_set_schedule_57 >> if_log_checkifthetimeoffisalreadyassigned_28_present_60
        if_name_downcase_equals_to_sickleave_44 >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_59 >> if_log_checkifthetimeoffisalreadyassigned_28_present_60
        if_log_13_present_43 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_28_present_60
        if_log_checkifthetimeoffisalreadyassigned_28_blank_29 >> rail.Label(
            'No') >> if_log_checkifthetimeoffisalreadyassigned_28_present_60
        if_log_checkifthetimeoffisalreadyassigned_28_present_60 >> rail.Label(
            'Yes') >> if_name_downcase_equals_to_sickleave_61
        if_name_downcase_equals_to_sickleave_61 >> rail.Label(
            'Yes') >> log_lookupbalancebasedonmonthofhire_62 >> if_log_lookupbalancebasedonmonthofhire_62_present_63
        if_log_lookupbalancebasedonmonthofhire_62_present_63 >> rail.Label(
            'Yes') >> get_default_time_off_type_policy_schedule_for_user_65 >> log_timeoff_policy_67 >> if_log_timeoff_policy_67_present_68
        if_log_timeoff_policy_67_present_68 >> rail.Label(
            'Yes') >> log_initial_balancefrompolicy_69 >> parse_json_70 >> log_initial_balance_71 >> log_valuefromdefault_72 >> log_valuetobe_gsubbed_73 >> declare_list_74 >> declare_list_75 >> log_existing_policy_76 >> if_log_existing_policy_76_present_77
        if_log_existing_policy_76_present_77 >> rail.Label(
            'Yes') >> parse_json_78 >> foreach_document_79 >> foreach_foreach_document_79_80 >> log_effectivedateforcomparison_81 >> if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_82
        if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_82 >> rail.Label(
            'Yes') >> insert_to_list_83 >> foreach_foreach_document_79_80_end
        if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_82 >> rail.Label(
            'No') >> foreach_foreach_document_79_80_end
        foreach_foreach_document_79_80 >> foreach_foreach_document_79_80_end >> foreach_document_79_end
        foreach_document_79 >> foreach_document_79_end >> get_defaultpolicyfromgloballevel_84
        if_log_existing_policy_76_present_77 >> rail.Label('No') >> get_defaultpolicyfromgloballevel_84 >> declare_list_85 \
            >> foreach_response_86 >> if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_87
        if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_87 >> rail.Label(
            'Yes') >> insert_to_list_88 >> foreach_response_86_end
        if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_87 >> rail.Label(
            'No') >> foreach_response_86_end

        if_log_timeoff_policy_67_present_68 >> rail.Label(
            'No') >> if_name_downcase_not_equals_to_sickleave_109

        foreach_response_86 >> foreach_response_86_end >> foreach_declare_list_85_89 >> log_policyset_90 >> if_foreach_declare_list_85_89_indexforeach_meta_equals_to_0_91
        if_foreach_declare_list_85_89_indexforeach_meta_equals_to_0_91 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_rehire_date_92 >> insert_to_list_93 >> foreach_declare_list_85_89_end
        if_foreach_declare_list_85_89_indexforeach_meta_equals_to_0_91 >> rail.Label(
            'No') >> log_required_effective_date_95 >> invoke_custom_ruby_code_required_effective_date_96 >> insert_to_list_97 >> foreach_declare_list_85_89_end
        foreach_declare_list_85_89 >> foreach_declare_list_85_89_end >> if_declare_list_75_list_items_equals_to_0_98
        if_declare_list_75_list_items_equals_to_0_98 >> rail.Label(
            'Yes') >> foreach_response_99 >> last_item_in_get_defaultpolicyfromgloballevel >> if_foreach_response_99_indexforeach_meta_equals_to_dataforeachforeach_response_99sizeforeach_meta1_100
        if_foreach_response_99_indexforeach_meta_equals_to_dataforeachforeach_response_99sizeforeach_meta1_100 >> rail.Label(
            'Yes') >> log_policyset_101 >> invoke_custom_ruby_code_rehire_date_102 >> insert_to_list_103 >> foreach_response_99_end
        if_foreach_response_99_indexforeach_meta_equals_to_dataforeachforeach_response_99sizeforeach_meta1_100 >> rail.Label(
            'No') >> foreach_response_99_end
        foreach_response_99 >> foreach_response_99_end >> log_existing_timeoff_policies_104

        if_declare_list_75_list_items_equals_to_0_98 >> rail.Label(
            'No') >> log_existing_timeoff_policies_104 >> log_new_timeoff_policies_105 >> log_new_policies_106 >> if_log_existing_timeoff_policies_104_present_107
        if_log_existing_timeoff_policies_104_present_107 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_108 >> if_name_downcase_not_equals_to_sickleave_109
        if_log_existing_timeoff_policies_104_present_107 >> rail.Label(
            'No') >> if_name_downcase_not_equals_to_sickleave_109

        if_log_lookupbalancebasedonmonthofhire_62_present_63 >> rail.Label(
            'No') >> if_name_downcase_not_equals_to_sickleave_109
        if_name_downcase_equals_to_sickleave_61 >> rail.Label(
            'No') >> if_name_downcase_not_equals_to_sickleave_109
        if_name_downcase_not_equals_to_sickleave_109 >> rail.Label(
            'Yes') >> get_defaultpolicyfromgloballevel_110 >> log_timeoff_policy_111 >> if_log_timeoff_policy_111_present_112
        if_log_timeoff_policy_111_present_112 >> rail.Label(
            'Yes') >> declare_list_113 >> log_existing_policy_114 >> if_log_existing_policy_114_present_115
        if_log_existing_policy_114_present_115 >> rail.Label(
            'Yes') >> parse_json_116 >> foreach_document_117 >> foreach_foreach_document_117_118 >> log_effectivedateforcomparison_119 >> if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_120
        if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_120 >> rail.Label(
            'Yes') >> insert_to_list_121 >> foreach_foreach_document_117_118_end
        if_to_time_less_than_dataworkato_servicereceive_requestrequestrehiredateto_time_120 >> rail.Label(
            'No') >> foreach_foreach_document_117_118_end
        foreach_foreach_document_117_118 >> foreach_foreach_document_117_118_end >> foreach_document_117_end
        foreach_document_117 >> foreach_document_117_end >> declare_list_122
        if_log_existing_policy_114_present_115 >> rail.Label(
            'No') >> declare_list_122 >> foreach_response_123 >> if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_124
        if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_124 >> rail.Label(
            'Yes') >> insert_to_list_125 >> accumulate_list_items_126 >> foreach_response_123_end
        if_startoffset_offsetvalue_equals_to_dataloggerlog_tenure_3message_124 >> rail.Label(
            'No') >> foreach_response_123_end
        foreach_response_123 >> foreach_response_123_end >> foreach_declare_list_122_127 >> if_foreach_declare_list_122_127_indexforeach_meta_equals_to_0_128

        if_foreach_declare_list_122_127_indexforeach_meta_equals_to_0_128 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_required_effective_date_129 >> insert_to_list_130 >> foreach_declare_list_122_127_end
        if_foreach_declare_list_122_127_indexforeach_meta_equals_to_0_128 >> rail.Label(
            'No') >> log_required_effective_date_132 >> invoke_custom_ruby_code_required_effective_date_133 >> insert_to_list_134 >> foreach_declare_list_122_127_end
        foreach_declare_list_122_127 >> foreach_declare_list_122_127_end >> if_accumulate_list_items_126_list_items_equals_to_0_135

        if_accumulate_list_items_126_list_items_equals_to_0_135 >> rail.Label(
            'Yes') >> foreach_response_136 >> if_foreach_response_136_indexforeach_meta_equals_to_dataforeachforeach_response_136sizeforeach_meta1_137
        if_foreach_response_136_indexforeach_meta_equals_to_dataforeachforeach_response_136sizeforeach_meta1_137 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_required_effective_date_138 >> insert_to_list_139 >> foreach_response_136_end
        if_foreach_response_136_indexforeach_meta_equals_to_dataforeachforeach_response_136sizeforeach_meta1_137 >> rail.Label(
            'No') >> foreach_response_136_end
        foreach_response_136 >> foreach_response_136_end >> log_timeoff_policies_140
        if_accumulate_list_items_126_list_items_equals_to_0_135 >> rail.Label(
            'No') >> log_timeoff_policies_140
        if_log_timeoff_policy_111_present_112 >> rail.Label(
            'No') >> log_timeoff_policies_140 >> if_log_timeoff_policies_140_present_141
        if_log_timeoff_policies_140_present_141 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_142 >> foreach_d_26_end
        if_log_timeoff_policies_140_present_141 >> rail.Label(
            'No') >> foreach_d_26_end
        if_name_downcase_not_equals_to_sickleave_109 >> rail.Label(
            'No') >> foreach_d_26_end
        foreach_d_26 >> foreach_d_26_end

        if_log_12_present_18 >> rail.Label('No') >> catch_error_143
        if_first_displaytext_present_11 >> rail.Label(
            'No') >> catch_error_143

        if_log_checkifthetimeoffisalreadyassigned_28_present_60 >> rail.Label(
            'No') >> foreach_d_26_end
        if_foreach_1_istimeoffallowedagainstthistimeofftype_is_true_27 >> rail.Label(
            'No') >> foreach_d_26_end
        foreach_d_26_end >> catch_error_143
        if_first_displaytext_present_11 >> rail.Label(
            'No') >> catch_error_143

    return dag


rail.for_each_instance(create_dag)
