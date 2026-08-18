from datetime import timedelta
import json
from airflow.models import Variable
import rail
from fujifilmdbtl.user_import.mapper.fujifilmdbtl_timeoff_balance_mapper import fdt_timeoff_balance_mapper
from fujifilmdbtl.user_import.utils.python_callable import get_timeoff_type_uris

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_child_add_timeoff_type_for_new_user_{config.instance}',
        description=f'FDT Child Workflow to add timeoff type for new user {config.instance}',
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
            no_task='_adhoc_http_action_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='_adhoc_http_action_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        _adhoc_http_action_3 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_3',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        if_first_displaytext_present_4 = rail.IfOperator(
            task_id='if_first_displaytext_present_4',
            test=lambda: rail.result('_adhoc_http_action_3')[0]['displayText'],
            yes_task="fdt_timeoff_balance_mapper_search_entries_5",
            no_task="finish",
        )

        fdt_timeoff_balance_mapper_search_entries_5 = rail.PythonOperator(
            task_id='fdt_timeoff_balance_mapper_search_entries_5',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "timeoff" and x["monthofhire"] == dag_run.conf['regulartemp'] and x["ftpt"] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper))
        )

        declare_list_6 = rail.SetVariableOperator(
            task_id='declare_list_6',
            append=False,
            name='timeoff_uris_to_assign',
            value=[]
        )

        foreach_fdt_timeoff_balance_mapper_search_entries_5_7 = rail.ForEachOperator(
            task_id='foreach_fdt_timeoff_balance_mapper_search_entries_5_7',
            items=lambda: rail.result(
                'fdt_timeoff_balance_mapper_search_entries_5'),
            start_task='insert_to_list_8',
            end_task='foreach_fdt_timeoff_balance_mapper_search_entries_5_7_end'
        )

        insert_to_list_8 = rail.SetVariableOperator(
            task_id='insert_to_list_8',
            append=True,
            name='timeoff_uris_to_assign',
            value=lambda: {
                "name": rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_5_7')['balance'],
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_3'), 'displayText', rail.result('foreach_fdt_timeoff_balance_mapper_search_entries_5_7')['balance'], 'uri', "")
            }
        )

        foreach_fdt_timeoff_balance_mapper_search_entries_5_7_end = rail.EmptyOperator(
            task_id='foreach_fdt_timeoff_balance_mapper_search_entries_5_7_end',
        )

        log_final_set_timeoff_uris_9 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_9',
            python_callable=lambda: get_timeoff_type_uris(
                rail.get_dag_run_var('timeoff_uris_to_assign'))
        )

        if_log_12_blank_10 = rail.IfOperator(
            task_id='if_log_12_blank_10',
            test='''{{ result('log_final_set_timeoff_uris_9') | is_falsy }}''',
            yes_task="put_time_off_type_assignments_for_user_11",
            no_task="if_log_12_present_12",
        )

        put_time_off_type_assignments_for_user_11 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_11',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUris": []
            }
        )

        if_log_12_present_12 = rail.IfOperator(
            task_id='if_log_12_present_12',
            test='''{{ result('log_final_set_timeoff_uris_9') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_13",
            no_task="finish",
        )

        put_time_off_type_assignments_for_user_13 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_13',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_9')
            }
        )

        foreach_declare_list_6_14 = rail.ForEachOperator(
            task_id='foreach_declare_list_6_14',
            items=lambda: rail.get_dag_run_var('timeoff_uris_to_assign'),
            start_task='if_foreach_1_uri_present_15',
            end_task='foreach_declare_list_6_14_end'
        )

        if_foreach_1_uri_present_15 = rail.IfOperator(
            task_id='if_foreach_1_uri_present_15',
            test='''{{ result('foreach_declare_list_6_14').uri | is_truthy }}''',
            yes_task="accumulate_list_items_16",
            no_task="foreach_declare_list_6_14_end",
        )

        accumulate_list_items_16 = rail.SetVariableOperator(
            task_id='accumulate_list_items_16',
            name='assigned_timeoff_types',
            append=True,
            value={
                "timeofftype": "{{result('foreach_declare_list_6_14')}}"
            }
        )

        get_default_time_off_type_policy_schedule_for_user_18 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_18',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_declare_list_6_14').uri }}"
                }
            }
        )

        log_timeoff_policy_20 = rail.PythonOperator(
            task_id='log_timeoff_policy_20',
            python_callable=lambda: json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_18')).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"') if rail.result('get_default_time_off_type_policy_schedule_for_user_18') else ""
        )

        if_log_13_present_21 = rail.IfOperator(
            task_id='if_log_13_present_21',
            test='''{{ result('log_timeoff_policy_20') | is_truthy }}''',
            yes_task="log_yearlyresetfrompolicy_22",
            no_task="foreach_declare_list_6_14_end",
        )

        log_yearlyresetfrompolicy_22 = rail.PythonOperator(
            task_id='log_yearlyresetfrompolicy_22',
            python_callable=lambda:  json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_type_policy_schedule_for_user_18')[0][
                                                'policySet']['timeOffBalanceEventScripts'], 'script.name', 'Yearly Reset', 'additionalParameters', "")).replace("[[", "[").replace("]]", "]")
        )

        parse_json_23 = rail.PythonOperator(
            task_id='parse_json_23',
            python_callable=lambda: json.loads(
                rail.result('log_yearlyresetfrompolicy_22'))
        )

        log_reset_balance_24 = rail.PythonOperator(
            task_id='log_reset_balance_24',
            python_callable=lambda: str(float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_23'), 'keyUri', 'urn:replicon:script-key:parameter:reset-balance-amount', 'value.number') if rail.result('parse_json_23') else 0))
        )

        log_default_resetamountfor_sick_leave_25 = rail.PythonOperator(
            task_id='log_default_resetamountfor_sick_leave_25',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                                                "value": {"number": float(rail.result('log_reset_balance_24'))}})
        )

        declare_variable_26 = rail.SetVariableOperator(
            task_id='declare_variable_26',
            append=False,
            name='sick_reset_amount',
            value=lambda: rail.result(
                'log_default_resetamountfor_sick_leave_25')
        )

        if_name_downcase_equals_to_sickleave_27 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_27',
            test=lambda dag_run: rail.result('foreach_declare_list_6_14')[
                'name'].lower() == 'sick leave' and dag_run.conf['ftpt'] == 'p',
            yes_task="log_default_resetamountfor_sick_leaveforparttime_28",
            no_task="if_name_downcase_equals_to_sickleave_30",
        )

        log_default_resetamountfor_sick_leaveforparttime_28 = rail.PythonOperator(
            task_id='log_default_resetamountfor_sick_leaveforparttime_28',
            python_callable=lambda:  str(
                float(float(rail.result('log_reset_balance_24')) / 2))
        )

        update_variable_29 = rail.SetVariableOperator(
            task_id='update_variable_29',
            append=False,
            name='sick_reset_amount',
            value=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                                      "value": {"number": float(rail.result('log_default_resetamountfor_sick_leaveforparttime_28'))}})
        )

        if_name_downcase_equals_to_sickleave_30 = rail.IfOperator(
            task_id='if_name_downcase_equals_to_sickleave_30',
            test=lambda: rail.result('foreach_declare_list_6_14')['name'].lower() == 'sick leave' or rail.result(
                'foreach_declare_list_6_14')['name'].lower() == 'floating holiday',
            yes_task="log_lookupbalancebasedonmonthofhire_31",
            no_task="put_user_time_off_account_policy_set_schedule_45",
        )

        log_lookupbalancebasedonmonthofhire_31 = rail.PythonOperator(
            task_id='log_lookupbalancebasedonmonthofhire_31',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == (rail.result('foreach_declare_list_6_14')['name']).lower() and x["monthofhire"] == dag_run.conf['startdatemonth'] and x['ftpt'] == dag_run.conf['ftpt'], fdt_timeoff_balance_mapper))[0]['balance']
        )

        if_log_lookupbalancebasedonmonthofhire_31_present_32 = rail.IfOperator(
            task_id='if_log_lookupbalancebasedonmonthofhire_31_present_32',
            test='''{{ result('log_lookupbalancebasedonmonthofhire_31') | is_truthy }}''',
            yes_task="log_initial_balancefrompolicy_33",
            no_task="log_timeoff_policy_42",
        )

        log_initial_balancefrompolicy_33 = rail.PythonOperator(
            task_id='log_initial_balancefrompolicy_33',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_default_time_off_type_policy_schedule_for_user_18')[0][
                                               'policySet']['timeOffBalanceEventScripts'], 'script.name', 'Starting Balance Set To', 'additionalParameters', "")).replace("[[", "[").replace("]]", "]")
        )

        parse_json_34 = rail.PythonOperator(
            task_id='parse_json_34',
            python_callable=lambda: json.loads(
                rail.result('log_initial_balancefrompolicy_33'))
        )

        log_initial_balance_35 = rail.PythonOperator(
            task_id='log_initial_balance_35',
            python_callable=lambda: str(float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_34'), 'keyUri', "urn:replicon:script-key:parameter:amount", 'value.number')))
        )

        log_valuefromdefault_36 = rail.PythonOperator(
            task_id='log_valuefromdefault_36',
            python_callable=lambda:  json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                 "value": {"number": float(rail.result('log_initial_balance_35'))}})
        )

        log_valuetobe_gsubbed_37 = rail.PythonOperator(
            task_id='log_valuetobe_gsubbed_37',
            python_callable=lambda:  json.dumps({"keyUri": "urn:replicon:script-key:parameter:amount",
                                                 "value": {"number": float(rail.result('log_lookupbalancebasedonmonthofhire_31'))}})
        )

        log_timeoff_policy_38 = rail.PythonOperator(
            task_id='log_timeoff_policy_38',
            python_callable=lambda:  json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_18')).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace(rail.result('log_valuefromdefault_36'), rail.result('log_valuetobe_gsubbed_37'))
        )

        log_timeoff_policy_39 = rail.PythonOperator(
            task_id='log_timeoff_policy_39',
            python_callable=lambda:  rail.result('log_timeoff_policy_38').replace(rail.result('log_default_resetamountfor_sick_leave_25'), rail.get_dag_run_var(
                'sick_reset_amount')) if rail.result('foreach_declare_list_6_14')['name'].lower() == "sick leave" else rail.result('log_timeoff_policy_38')
        )

        put_user_time_off_account_policy_set_schedule_40 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_40',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_declare_list_6_14')['uri']
                },
                "policySetScheduleEntries": json.loads(rail.result('log_timeoff_policy_39'))
            }
        )

        log_timeoff_policy_42 = rail.PythonOperator(
            task_id='log_timeoff_policy_42',
            python_callable=lambda: json.loads(rail.result('log_timeoff_policy_20').replace(rail.result('log_default_resetamountfor_sick_leave_25'), rail.get_dag_run_var(
                'sick_reset_amount')) if rail.result('foreach_declare_list_6_14')['name'].lower() == "sick leave" else rail.result('log_timeoff_policy_20'))
        )

        put_user_time_off_account_policy_set_schedule_43 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_43',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_declare_list_6_14')['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_42')
            }
        )

        put_user_time_off_account_policy_set_schedule_45 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_45',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_declare_list_6_14')['uri']
                },
                "policySetScheduleEntries": json.loads(rail.result('log_timeoff_policy_20'))
            }
        )

        foreach_declare_list_6_14_end = rail.EmptyOperator(
            task_id='foreach_declare_list_6_14_end',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> _adhoc_http_action_3
        _adhoc_http_action_3 >> if_first_displaytext_present_4
        if_first_displaytext_present_4 >> rail.Label('Yes') >> fdt_timeoff_balance_mapper_search_entries_5 >> declare_list_6 \
            >> foreach_fdt_timeoff_balance_mapper_search_entries_5_7 >> insert_to_list_8 >> foreach_fdt_timeoff_balance_mapper_search_entries_5_7_end
        foreach_fdt_timeoff_balance_mapper_search_entries_5_7 >> foreach_fdt_timeoff_balance_mapper_search_entries_5_7_end \
            >> log_final_set_timeoff_uris_9 >> if_log_12_blank_10
        if_log_12_blank_10 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_11 >> if_log_12_present_12
        if_log_12_blank_10 >> rail.Label('No') >> if_log_12_present_12
        if_log_12_present_12 >> rail.Label('Yes') >> put_time_off_type_assignments_for_user_13 >> foreach_declare_list_6_14 \
            >> if_foreach_1_uri_present_15 >> rail.Label('Yes') >> accumulate_list_items_16 >> get_default_time_off_type_policy_schedule_for_user_18 \
            >> log_timeoff_policy_20 >> if_log_13_present_21
        if_log_13_present_21 >> rail.Label('Yes') >> log_yearlyresetfrompolicy_22 >> parse_json_23 >> log_reset_balance_24 \
            >> log_default_resetamountfor_sick_leave_25 >> declare_variable_26 >> if_name_downcase_equals_to_sickleave_27
        if_name_downcase_equals_to_sickleave_27 >> rail.Label('Yes') >> log_default_resetamountfor_sick_leaveforparttime_28 \
            >> update_variable_29 >> if_name_downcase_equals_to_sickleave_30
        if_name_downcase_equals_to_sickleave_27 >> rail.Label(
            'No') >> if_name_downcase_equals_to_sickleave_30
        if_name_downcase_equals_to_sickleave_30 >> rail.Label('Yes') >> log_lookupbalancebasedonmonthofhire_31 \
            >> if_log_lookupbalancebasedonmonthofhire_31_present_32 >> rail.Label('Yes') >> log_initial_balancefrompolicy_33 \
            >> parse_json_34 >> log_initial_balance_35 >> log_valuefromdefault_36 >> log_valuetobe_gsubbed_37 >> log_timeoff_policy_38 \
            >> log_timeoff_policy_39 >> put_user_time_off_account_policy_set_schedule_40 >> foreach_declare_list_6_14_end
        if_log_lookupbalancebasedonmonthofhire_31_present_32 >> rail.Label('No') >> log_timeoff_policy_42 \
            >> put_user_time_off_account_policy_set_schedule_43 >> foreach_declare_list_6_14_end
        if_name_downcase_equals_to_sickleave_30 >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_45 >> foreach_declare_list_6_14_end
        if_log_13_present_21 >> rail.Label(
            'No') >> foreach_declare_list_6_14_end
        if_foreach_1_uri_present_15 >> rail.Label(
            'No') >> foreach_declare_list_6_14_end
        foreach_declare_list_6_14 >> foreach_declare_list_6_14_end >> finish
        if_log_12_present_12 >> rail.Label('No') >> finish
        if_first_displaytext_present_4 >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_dag)
