from datetime import timedelta
from airflow.models import Variable
from assuredpartnersinc.user_import_v3.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_workflow_to_add_timeoff_type_for_transfer_dag_id,
        description=f'Assured Partners User Import Add time off type for Transfer Child{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_variable_6'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable_6',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_variable_6 = rail.SetVariableOperator(
            task_id='declare_variable_6',
            append=False,
            name='previous_pto_1_name',
            value=None
        )

        declare_variable_7 = rail.SetVariableOperator(
            task_id='declare_variable_7',
            append=False,
            name='effectivedate_for_stop_accruals',
            value=lambda dag_run: python_callable.get_split_date(
                python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'no_split') - timedelta(days=1), 'int')
        )

        get_required_script_uri_from_timeoffbalanceevent_scripts = rail.RepliconServiceOperator(
            task_id='get_required_script_uri_from_timeoffbalanceevent_scripts',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: {
                'starting_balance_set_to_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', "Starting Balance Set To", 'uri') if response else null,
                'yearly_reset_script_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Yearly Reset', 'uri'),
                'max_balance_limit_script_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Max Balance Limit', 'uri')
            }
        )

        log_tenure_basedon_pto_seniority_date = rail.PythonOperator(
            task_id='log_tenure_basedon_pto_seniority_date',
            python_callable=lambda dag_run:  python_callable.get_user_tenure_in_years(
                dag_run.conf['PTOSeniorityDate'], dag_run.conf['ChangeEffectiveDate'], dag_run) if dag_run.conf['PTOSeniorityDate'] else 0
        )

        get_time_off_type_assignments_for_user_9 = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user_9',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_10 = rail.PythonOperator(
            task_id='log_10',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_time_off_type_assignments_for_user_9'), "displayText",  "Sick Pay-H", 'uri')
        )

        if_log_10_present_sick_pay_h_not_eligible_anymore_12 = rail.IfOperator(
            task_id='if_log_10_present_sick_pay_h_not_eligible_anymore_12',
            test='''{{ result('log_10') | is_truthy  and dag_run.conf.Illness | is_falsy }}''',
            yes_task="trigger_dag_run_child_update_sick_pay_h_policy_on_ineligibity_13",
            no_task="update_variable_18",
        )

        trigger_dag_run_child_update_sick_pay_h_policy_on_ineligibity_13 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_update_sick_pay_h_policy_on_ineligibity_13',
            retries=0,
            trigger_dag_id=config.child_update_sick_pay_h_policy_on_ineligibity_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('log_10'),
                "timeofftypename": dag_run.conf['Illness'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "update",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "enddate": dag_run.conf['LOASuspendPTOStart'] if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate'],
                "previousptoname": rail.get_dag_run_var('previous_pto_1_name'),
                "pto_1": dag_run.conf['PTO_1'],
                "estatus": dag_run.conf['EEStatus'],
                "illness": dag_run.conf['Illness'],
                "loastartdate": dag_run.conf['LOASuspendPTOStart'] if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        wait_for_completion_trigger_dag_run_child_update_sick_pay_h_policy_on_ineligibity_13 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_update_sick_pay_h_policy_on_ineligibity_13',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_update_sick_pay_h_policy_on_ineligibity_13") }}'
        )

        def get_previous_pto1_name_from_user_timeoff_assignments(user_timeoff_type_assignments):
            for item in user_timeoff_type_assignments:
                matching_timeoff_name_in_mapper = list(filter(
                    lambda x: x['time_off_type_name'] == item['name'], config.TO_PTO1_MAPPER))
                if matching_timeoff_name_in_mapper:
                    for data in matching_timeoff_name_in_mapper:
                        if data['time_off_type_name'] != 'Sick Pay-P':
                            return data['time_off_type_name']
            return null

        update_variable_18 = rail.SetVariableOperator(
            task_id='update_variable_18',
            append=False,
            name='previous_pto_1_name',
            value=lambda: get_previous_pto1_name_from_user_timeoff_assignments(
                rail.result('get_time_off_type_assignments_for_user_9'))
        )

        def get_value_for_stop_accruals(previous_pto1_name, dag_run):
            if previous_pto1_name and not (dag_run.conf['PTO_1']) and dag_run.conf['EEStatus'] != 'T':
                return "yes"
            if previous_pto1_name != dag_run.conf['PTO_1'] and dag_run.conf['EEStatus'] != 'T':
                return "yes"
            return "no"

        log_get_value_for_stop_accruals_and_previous_pto1_timeofftype_uri = rail.PythonOperator(
            task_id='log_get_value_for_stop_accruals_and_previous_pto1_timeofftype_uri',
            python_callable=lambda dag_run: {
                'value_for_stop_accruals': get_value_for_stop_accruals(rail.get_dag_run_var('previous_pto_1_name'), dag_run),
                'previous_pto1_timeoff_type_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_time_off_type_assignments_for_user_9'), 'displayText', rail.get_dag_run_var('previous_pto_1_name'), 'uri') if rail.result(
                        'get_time_off_type_assignments_for_user_9') else null
            }
        )

        if_request_loa_stop_accruals_equals_to_yes_24 = rail.IfOperator(
            task_id='if_request_loa_stop_accruals_equals_to_yes_24',
            test='''{{ dag_run.conf.loa_stop_accruals == 'yes' }}''',
            yes_task="update_variable_25",
            no_task="if_declare_variable_8_value_equals_to_yes_32",
        )

        update_variable_25 = rail.SetVariableOperator(
            task_id='update_variable_25',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value=lambda dag_run: python_callable.get_split_date(
                dag_run.conf['LOASuspendPTOStart'], 'int') if dag_run.conf['LOASuspendPTOStart'] else python_callable.get_split_date(
                    python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'no_split') - timedelta(days=1), 'int')
        )

        if_log_previous_p_t_o1timeofftype_uri_23_present_27 = rail.IfOperator(
            task_id='if_log_previous_p_t_o1timeofftype_uri_23_present_27',
            test='''{{ result('log_get_value_for_stop_accruals_and_previous_pto1_timeofftype_uri').previous_pto1_timeoff_type_uri | is_truthy }}''',
            yes_task="trigger_dag_run_child_update_pto_policy_on_loa_28",
            no_task="if_log_10_present_29",
        )

        trigger_dag_run_child_update_pto_policy_on_loa_28 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_update_pto_policy_on_loa_28',
            retries=0,
            trigger_dag_id=config.child_update_pto_policy_on_loa_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('log_get_value_for_stop_accruals_and_previous_pto1_timeofftype_uri')['previous_pto1_timeoff_type_uri'],
                "timeofftypename": rail.get_dag_run_var('previous_pto_1_name'),
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "loa",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "enddate": dag_run.conf['LOASuspendPTOStart'] if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate'],
                "previousptoname": rail.get_dag_run_var('previous_pto_1_name'),
                "pto_1": dag_run.conf['PTO_1'],
                "estatus": dag_run.conf['EEStatus'],
                "illness": dag_run.conf['Illness'],
                "loastartdate": dag_run.conf['LOASuspendPTOStart'] if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate'],
                'yearly_reset_script_uri': rail.result('get_required_script_uri_from_timeoffbalanceevent_scripts')['yearly_reset_script_uri'],
                'max_balance_limit_script_uri': rail.result('get_required_script_uri_from_timeoffbalanceevent_scripts')['max_balance_limit_script_uri'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        wait_for_completion_trigger_dag_run_child_update_pto_policy_on_loa_28 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_update_pto_policy_on_loa_28',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_update_pto_policy_on_loa_28") }}'
        )

        if_log_10_present_29 = rail.IfOperator(
            task_id='if_log_10_present_29',
            test='''{{ result('log_10') | is_truthy }}''',
            yes_task="trigger_dag_run_child_update_sick_pay_h_policy_on_loa_30",
            no_task="if_declare_variable_8_value_equals_to_yes_32",
        )

        trigger_dag_run_child_update_sick_pay_h_policy_on_loa_30 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_update_sick_pay_h_policy_on_loa_30',
            retries=0,
            trigger_dag_id=config.child_update_sick_pay_h_policy_on_loa_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('log_10'),
                "timeofftypename": dag_run.conf['Illness'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "loa",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "enddate": dag_run.conf['LOASuspendPTOStart'] if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate'],
                "previousptoname": rail.get_dag_run_var('previous_pto_1_name'),
                "pto_1": dag_run.conf['PTO_1'],
                "estatus": dag_run.conf['EEStatus'],
                "illness": dag_run.conf['Illness'],
                "loastartdate": dag_run.conf['LOASuspendPTOStart'] if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate'],
                'yearly_reset_script_uri': rail.result('get_required_script_uri_from_timeoffbalanceevent_scripts')['yearly_reset_script_uri'],
                'max_balance_limit_script_uri': rail.result('get_required_script_uri_from_timeoffbalanceevent_scripts')['max_balance_limit_script_uri'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        wait_for_completion_trigger_dag_run_child_update_sick_pay_h_policy_on_loa_30 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_update_sick_pay_h_policy_on_loa_30',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_update_sick_pay_h_policy_on_loa_30") }}'
        )

        if_declare_variable_8_value_equals_to_yes_32 = rail.IfOperator(
            task_id='if_declare_variable_8_value_equals_to_yes_32',
            test='''{{ result('log_get_value_for_stop_accruals_and_previous_pto1_timeofftype_uri').value_for_stop_accruals == 'yes' }}''',
            yes_task="if_log_previous_p_t_o1timeofftype_uri_23_present_33",
            no_task="if_request_loa_stop_accruals_equals_to_yes_41",
        )

        if_log_previous_p_t_o1timeofftype_uri_23_present_33 = rail.IfOperator(
            task_id='if_log_previous_p_t_o1timeofftype_uri_23_present_33',
            test='''{{ result('log_get_value_for_stop_accruals_and_previous_pto1_timeofftype_uri').previous_pto1_timeoff_type_uri | is_truthy }}''',
            yes_task="get_user_time_off_type_balance_summary_35",
            no_task="if_request_loa_stop_accruals_equals_to_yes_41",
        )

        get_user_time_off_type_balance_summary_35 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_balance_summary_35',
            endpoint="/services/TimeOffService1.svc/GetUserTimeOffTypeBalanceSummary",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUri": rail.result('log_get_value_for_stop_accruals_and_previous_pto1_timeofftype_uri')['previous_pto1_timeoff_type_uri'],
                "asOfDate": rail.get_dag_run_var('effectivedate_for_stop_accruals')
            }
        )

        log_balance_minutes_and_seconds_converted_to_balance_hours = rail.PythonOperator(
            task_id='log_balance_minutes_and_seconds_converted_to_balance_hours',
            python_callable=lambda: {
                'minutes_to_hours': ((float(rail.result(
                    'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['minutes']) / 60) if int(rail.result(
                        'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['minutes']) > 0 else 0) if rail.result(
                            'get_user_time_off_type_balance_summary_35') else 0,
                'seconds_to_hours': ((float(rail.result(
                    'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['seconds']) / 3600) if int(rail.result(
                        'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['seconds']) > 0 else 0) if rail.result(
                            'get_user_time_off_type_balance_summary_35') else 0
            }
        )

        trigger_dag_run_child_remove_future_time_off_policies_transfer_and_termination_40 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_remove_future_time_off_policies_transfer_and_termination_40',
            retries=0,
            trigger_dag_id=config.child_remove_future_timeoff_policies_transfer_termination_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('log_get_value_for_stop_accruals_and_previous_pto1_timeofftype_uri')['previous_pto1_timeoff_type_uri'],
                "timeofftypename": rail.get_dag_run_var('previous_pto_1_name'),
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "loa",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance":  (float(rail.result('get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result('log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'get_user_time_off_type_balance_summary_35') else 0,
                "enddate": (dag_run.conf['LOASuspendPTOStart'] if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate']) if dag_run.conf['loa_stop_accruals'] == "yes" else dag_run.conf['ChangeEffectiveDate'],
                "starting_balance_set_to_uri": rail.result('get_required_script_uri_from_timeoffbalanceevent_scripts')['starting_balance_set_to_uri'],
                "previousptoname": rail.get_dag_run_var('previous_pto_1_name'),
                "pto_1": dag_run.conf['PTO_1'],
                "estatus": dag_run.conf['EEStatus'],
                "illness": dag_run.conf['Illness'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        wait_for_completion_trigger_dag_run_child_remove_future_time_off_policies_transfer_and_termination_40 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_remove_future_time_off_policies_transfer_and_termination_40',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_remove_future_time_off_policies_transfer_and_termination_40") }}'
        )

        if_request_loa_stop_accruals_equals_to_yes_41 = rail.IfOperator(
            task_id='if_request_loa_stop_accruals_equals_to_yes_41',
            test='''{{ dag_run.conf.loa_stop_accruals == 'yes' }}''',
            yes_task="catch_and_log_error",
            no_task="get_all_timeoff_types_43",
        )

        get_all_timeoff_types_43 = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_types_43',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        if_first_displaytext_present_44 = rail.IfOperator(
            task_id='if_first_displaytext_present_44',
            test=lambda: rail.result("get_all_timeoff_types_43"),
            yes_task="get_timeoff_uri_name_list",
            no_task="catch_and_log_error",
        )

        get_timeoff_uri_name_list = rail.PythonOperator(
            task_id='get_timeoff_uri_name_list',
            python_callable=lambda dag_run: python_callable.final_timeoffs_to_be_added_list(
                dag_run, rail.result("get_all_timeoff_types_43"))
        )

        def add_pto_payout_to_final_list(dag_run, final_timeoff_list, previous_pto1_name, all_timeoffs_list):
            if dag_run.conf['EEStatus'] == 'T':
                if previous_pto1_name:
                    final_timeoff_list.append({
                        "name": "PTO Payout",
                        "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', "PTO Payout", 'uri')
                    })
            if dag_run.conf['EEStatus'] != 'T':
                if not (dag_run.conf['PTO_1']) and previous_pto1_name and dag_run.conf['Illness'] == "Sick Pay-H":
                    final_timeoff_list.append({
                        "name": "PTO Payout",
                        "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', "PTO Payout", 'uri')
                    })
            return {
                'final_timeoff_list': final_timeoff_list,
                'final_timeoff_uris_to_assign': [item['uri'] for item in final_timeoff_list]
            }

        log_final_set_timeoff_uris_79 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_79',
            python_callable=lambda dag_run: add_pto_payout_to_final_list(dag_run, rail.result('get_timeoff_uri_name_list'), rail.get_dag_run_var(
                'previous_pto_1_name'), rail.result('get_all_timeoff_types_43'))
        )

        if_log_12_present_80 = rail.IfOperator(
            task_id='if_log_12_present_80',
            test='''{{ result('log_final_set_timeoff_uris_79').final_timeoff_uris_to_assign | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_81",
            no_task="catch_and_log_error",
        )

        put_time_off_type_assignments_for_user_81 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_81',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_79')['final_timeoff_uris_to_assign']
            }
        )

        assured_partners_pto_1_time_off_list_search_entries_82 = rail.PythonOperator(
            task_id='assured_partners_pto_1_time_off_list_search_entries_82',
            python_callable=lambda:  list(
                filter(lambda x: x["identifier"] == "timeoff", config.TO_PTO1_MAPPER))
        )

        log_pto1_timeofflist_h_83 = rail.PythonOperator(
            task_id='log_pto1_timeofflist_h_83',
            python_callable=lambda:  [item['time_off_type_name'] for item in rail.result(
                "assured_partners_pto_1_time_off_list_search_entries_82")]
        )

        create_child_triggered_list = rail.SetVariableOperator(
            task_id='create_child_triggered_list',
            name='wait_for_dag_runs',
            append=False,
            value=[]
        )

        foreach_declare_list_45_84 = rail.ForEachOperator(
            task_id='foreach_declare_list_45_84',
            items=lambda: rail.result('log_final_set_timeoff_uris_79')[
                'final_timeoff_list'],
            start_task='if_foreach_1_uri_present_85',
            end_task='foreach_declare_list_45_84_end'
        )

        if_foreach_1_uri_present_85 = rail.IfOperator(
            task_id='if_foreach_1_uri_present_85',
            test='''{{ result('foreach_declare_list_45_84').uri | is_truthy }}''',
            yes_task="log_checkiftimeoffwaspreviouslyassigned_86",
            no_task="foreach_declare_list_45_84_end",
        )

        log_checkiftimeoffwaspreviouslyassigned_86 = rail.PythonOperator(
            task_id='log_checkiftimeoffwaspreviouslyassigned_86',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_time_off_type_assignments_for_user_9'), 'uri', rail.result(
                'foreach_declare_list_45_84')['uri'], 'name') if rail.result('get_time_off_type_assignments_for_user_9') else null
        )

        accumulate_list_items_87 = rail.SetVariableOperator(
            task_id='accumulate_list_items_87',
            name='assigned_timeoff_types',
            append=True,
            value={
                    "timeofftype": "{{ result('foreach_declare_list_45_84').name }}"
            }
        )

        if_log_pto1_timeofflist_h_83_not_contains_foreach_declare_list_45_84_name = rail.IfOperator(
            task_id='if_log_pto1_timeofflist_h_83_not_contains_foreach_declare_list_45_84_name',
            test=lambda: bool(rail.result('foreach_declare_list_45_84')[
                'name'] not in rail.result('log_pto1_timeofflist_h_83')),
            yes_task="if_request_loa_return_equals_to_yes_89",
            no_task="log_p_t_o_type_97",
        )

        if_request_loa_return_equals_to_yes_89 = rail.IfOperator(
            task_id='if_request_loa_return_equals_to_yes_89',
            test='''{{ dag_run.conf.loa_return == 'yes' }}''',
            yes_task="if_foreach_1_name_equals_to_sickpayh_90",
            no_task="if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93",
        )

        if_foreach_1_name_equals_to_sickpayh_90 = rail.IfOperator(
            task_id='if_foreach_1_name_equals_to_sickpayh_90',
            test='''{{ result('foreach_declare_list_45_84').name == 'Sick Pay-H' }}''',
            yes_task="if_log_checkiftimeoffwaspreviouslyassigned_86_present_91",
            no_task="if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93",
        )

        if_log_checkiftimeoffwaspreviouslyassigned_86_present_91 = rail.IfOperator(
            task_id='if_log_checkiftimeoffwaspreviouslyassigned_86_present_91',
            test='''{{ result('log_checkiftimeoffwaspreviouslyassigned_86') | is_truthy }}''',
            yes_task="trigger_dag_run_child_user_sick_pay_h_policy_update_92",
            no_task="if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93",
        )

        trigger_dag_run_child_user_sick_pay_h_policy_update_92 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_user_sick_pay_h_policy_update_92',
            retries=0,
            trigger_dag_id=config.child_user_sick_pay_h_policy_update_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "loa" if dag_run.conf['loa_return'] == "yes" else dag_run.conf['type'],
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "starting_balance_script_uri": rail.result('get_required_script_uri_from_timeoffbalanceevent_scripts')['starting_balance_set_to_uri'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_92_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_92_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_user_sick_pay_h_policy_update_92')}}"
        )

        if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93 = rail.IfOperator(
            task_id='if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93',
            test='''{{ result('log_checkiftimeoffwaspreviouslyassigned_86') | is_falsy }}''',
            yes_task="if_foreach_1_name_not_equals_to_ptopayout_94",
            no_task="foreach_declare_list_45_84_end",
        )

        if_foreach_1_name_not_equals_to_ptopayout_94 = rail.IfOperator(
            task_id='if_foreach_1_name_not_equals_to_ptopayout_94',
            test='''{{ result('foreach_declare_list_45_84').name != 'PTO Payout' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_95",
            no_task="foreach_declare_list_45_84_end",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_95 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_95',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "loa" if dag_run.conf['loa_return'] == "yes" else dag_run.conf['type'],
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_95_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_95_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_95')}}"
        )

        log_p_t_o_type_97 = rail.PythonOperator(
            task_id='log_p_t_o_type_97',
            python_callable=lambda: next(iter(filter(lambda x: x["time_off_type_name"] == rail.result(
                'foreach_declare_list_45_84')['name'], rail.result("assured_partners_pto_1_time_off_list_search_entries_82"))), {}).get('type', '')
        )

        if_request_loa_return_equals_to_yes_98 = rail.IfOperator(
            task_id='if_request_loa_return_equals_to_yes_98',
            test='''{{ dag_run.conf.loa_return == 'yes' }}''',
            yes_task="if_log_p_t_o_type_97_equals_to_type1_99",
            no_task="if_request_pto_1_not_equals_to_declare_variable_6_value_106",
        )

        if_log_p_t_o_type_97_equals_to_type1_99 = rail.IfOperator(
            task_id='if_log_p_t_o_type_97_equals_to_type1_99',
            test='''{{ result('log_p_t_o_type_97') == 'Type 1' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_100",
            no_task="if_log_p_t_o_type_97_equals_to_type2_101",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_100 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_100',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "loa",
                "previousstartdate": dag_run.conf['LOASuspendPTOStart'] if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate'],
                "previousbalance": (float(rail.result('get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result('log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if (rail.result(
                        'get_user_time_off_type_balance_summary_35') and rail.result(
                        'get_user_time_off_type_balance_summary_35')['timeRemaining'] and rail.result(
                        'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration'] and rail.result(
                        'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) else 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['ChangeEffectiveDate'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_100_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_100_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_100')}}"
        )

        if_log_p_t_o_type_97_equals_to_type2_101 = rail.IfOperator(
            task_id='if_log_p_t_o_type_97_equals_to_type2_101',
            test='''{{ result('log_p_t_o_type_97') == 'Type 2' }}''',
            yes_task="if_foreach_1_name_equals_to_keenannoncaex_102",
            no_task="if_request_pto_1_not_equals_to_declare_variable_6_value_106",
        )

        if_foreach_1_name_equals_to_keenannoncaex_102 = rail.IfOperator(
            task_id='if_foreach_1_name_equals_to_keenannoncaex_102',
            test='''{{ result('foreach_declare_list_45_84').name == 'Keenan Non-CA EX'  or result('foreach_declare_list_45_84').name == 'Keenan Non-CA H' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_103",
            no_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_105",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_103 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_103',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "loa",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": (float(rail.result('get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result('log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'get_user_time_off_type_balance_summary_35') else 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['ChangeEffectiveDate'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_103_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_103_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_103')}}"
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_105 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_105',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "loa",
                "previousstartdate": dag_run.conf['LOASuspendPTOEnd'],
                "previousbalance": (float(rail.result('get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result('log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'get_user_time_off_type_balance_summary_35') else 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['ChangeEffectiveDate'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_105_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_105_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_105')}}"
        )

        if_request_pto_1_not_equals_to_declare_variable_6_value_106 = rail.IfOperator(
            task_id='if_request_pto_1_not_equals_to_declare_variable_6_value_106',
            test=lambda dag_run: dag_run.conf['PTO_1'] != rail.get_dag_run_var(
                'previous_pto_1_name'),
            yes_task="log_minutesconvertedtohours_107",
            no_task="if_log_check_if_timeoff_was_not_previously_assigned_86_blank_118",
        )

        log_minutesconvertedtohours_107 = rail.PythonOperator(
            task_id='log_minutesconvertedtohours_107',
            python_callable=lambda: (((int(rail.result(
                    'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['minutes']) / 60) if int(rail.result(
                        'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['minutes']) > 0 else 0) if rail.result(
                            'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['minutes'] else 0) if rail.result(
                'get_user_time_off_type_balance_summary_35') else 0
        )

        if_log_p_t_o_type_97_equals_to_type1_108 = rail.IfOperator(
            task_id='if_log_p_t_o_type_97_equals_to_type1_108',
            test='''{{ result('log_p_t_o_type_97') == 'Type 1' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_109",
            no_task="if_log_p_t_o_type_97_equals_to_type2_110",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_109 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_109',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "transfer",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": (float(rail.result('get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result('log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'get_user_time_off_type_balance_summary_35') else 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_109_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_109_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_109')}}"
        )

        if_log_p_t_o_type_97_equals_to_type2_110 = rail.IfOperator(
            task_id='if_log_p_t_o_type_97_equals_to_type2_110',
            test='''{{ result('log_p_t_o_type_97') == 'Type 2' }}''',
            yes_task="if_foreach_1_name_equals_to_keenannoncah_111",
            no_task="if_log_p_t_o_type_97_equals_to_sickpto_115",
        )

        if_foreach_1_name_equals_to_keenannoncah_111 = rail.IfOperator(
            task_id='if_foreach_1_name_equals_to_keenannoncah_111',
            test='''{{ result('foreach_declare_list_45_84').name == 'Keenan Non-CA H'  or result('foreach_declare_list_45_84').name == 'Keenan Non-CA EX' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_112",
            no_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_114",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_112 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_112',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "transfer",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": (float(rail.result('get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result('log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'get_user_time_off_type_balance_summary_35') else 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_112_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_112_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_112')}}"
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_114 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_114',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "transfer",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": (float(rail.result(
                    'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result(
                        'log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'get_user_time_off_type_balance_summary_35') else 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_114_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_114_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_114')}}"
        )

        if_log_p_t_o_type_97_equals_to_sickpto_115 = rail.IfOperator(
            task_id='if_log_p_t_o_type_97_equals_to_sickpto_115',
            test='''{{ result('log_p_t_o_type_97') == 'Sick PTO' }}''',
            yes_task="if_flsa_change_not_present_or_schedule_changed_or_new_eligibility",
            no_task="foreach_declare_list_45_84_end",
        )

        if_flsa_change_not_present_or_schedule_changed_or_new_eligibility = rail.IfOperator(
            task_id='if_flsa_change_not_present_or_schedule_changed_or_new_eligibility',
            test=lambda dag_run: not (dag_run.conf['flsa_changed']) or dag_run.conf['schedulechange'] == 'yes' or not (rail.result(
                'log_checkiftimeoffwaspreviouslyassigned_86')),
            yes_task="trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_116",
            no_task="foreach_declare_list_45_84_end",
        )

        trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_116 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_116',
            retries=0,
            trigger_dag_id=config.child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['integration_run_date'],
                "servicedate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "update",
                "currentschedule": dag_run.conf['currentschedule'],
                "currentscheduleuri": dag_run.conf['currentscheduleuri'],
                "schedulechange": dag_run.conf['schedulechange'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_116_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_116_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_116')}}"
        )

        if_log_check_if_timeoff_was_not_previously_assigned_86_blank_118 = rail.IfOperator(
            task_id='if_log_check_if_timeoff_was_not_previously_assigned_86_blank_118',
            test='''{{ result('log_checkiftimeoffwaspreviouslyassigned_86') | is_falsy }}''',
            yes_task="if_log_p_t_o_type_97_equals_to_type1_120",
            no_task="if_request_schedulechange_equals_to_yes_130",
        )

        if_log_p_t_o_type_97_equals_to_type1_120 = rail.IfOperator(
            task_id='if_log_p_t_o_type_97_equals_to_type1_120',
            test='''{{ result('log_p_t_o_type_97') == 'Type 1' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_121",
            no_task="if_log_p_t_o_type_97_equals_to_type2_122",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_121 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_121',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": dag_run.conf['type'],
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": (float(rail.result(
                    'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'get_user_time_off_type_balance_summary_35') else 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_121_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_121_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_121')}}"
        )

        if_log_p_t_o_type_97_equals_to_type2_122 = rail.IfOperator(
            task_id='if_log_p_t_o_type_97_equals_to_type2_122',
            test='''{{ result('log_p_t_o_type_97') == 'Type 2' }}''',
            yes_task="if_foreach_1_name_equals_to_keenannoncah_123",
            no_task="if_log_p_t_o_type_97_equals_to_sickpto_127",
        )

        if_foreach_1_name_equals_to_keenannoncah_123 = rail.IfOperator(
            task_id='if_foreach_1_name_equals_to_keenannoncah_123',
            test='''{{ result('foreach_declare_list_45_84').name == 'Keenan Non-CA H'  or result('foreach_declare_list_45_84').name == 'Keenan Non-CA EX' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_124",
            no_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_126",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_124 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_124',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": dag_run.conf['type'],
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": (float(rail.result(
                    'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'get_user_time_off_type_balance_summary_35') else 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_124_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_124_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_124')}}"
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_126 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_126',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": dag_run.conf['type'],
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": (float(rail.result(
                    'get_user_time_off_type_balance_summary_35')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'get_user_time_off_type_balance_summary_35') else 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_126_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_126_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_126')}}"
        )

        if_log_p_t_o_type_97_equals_to_sickpto_127 = rail.IfOperator(
            task_id='if_log_p_t_o_type_97_equals_to_sickpto_127',
            test='''{{ result('log_p_t_o_type_97') == 'Sick PTO' }}''',
            yes_task="if_flsa_change_not_present_or_schedule_changed_or_new_eligibility_check_2",
            no_task="foreach_declare_list_45_84_end",
        )

        if_flsa_change_not_present_or_schedule_changed_or_new_eligibility_check_2 = rail.IfOperator(
            task_id='if_flsa_change_not_present_or_schedule_changed_or_new_eligibility_check_2',
            test=lambda dag_run: not (dag_run.conf['flsa_changed']) or dag_run.conf['schedulechange'] == 'yes' or not (rail.result(
                'log_checkiftimeoffwaspreviouslyassigned_86')),
            yes_task="trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_128",
            no_task="foreach_declare_list_45_84_end",
        )

        trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_128 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_128',
            retries=0,
            trigger_dag_id=config.child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['integration_run_date'],
                "servicedate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "update",
                "currentschedule": dag_run.conf['currentschedule'],
                "currentscheduleuri": dag_run.conf['currentscheduleuri'],
                "schedulechange": dag_run.conf['schedulechange'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_128_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_128_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_128')}}"
        )

        if_request_schedulechange_equals_to_yes_130 = rail.IfOperator(
            task_id='if_request_schedulechange_equals_to_yes_130',
            test='''{{ dag_run.conf.schedulechange == 'yes' }}''',
            yes_task="if_foreach_1_name_equals_to_sickpayp_131",
            no_task="foreach_declare_list_45_84_end",
        )

        if_foreach_1_name_equals_to_sickpayp_131 = rail.IfOperator(
            task_id='if_foreach_1_name_equals_to_sickpayp_131',
            test='''{{ result('foreach_declare_list_45_84').name == 'Sick Pay-P' }}''',
            yes_task="trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_132",
            no_task="trigger_dag_run_child_workflow_for_change_in_schedule_and_pto_1_policy_update_134",
        )

        trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_132 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_132',
            retries=0,
            trigger_dag_id=config.child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "startdate": dag_run.conf['integration_run_date'],
                "servicedate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeoffuri": rail.result('foreach_declare_list_45_84')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_45_84')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "update",
                "currentschedule": dag_run.conf['currentschedule'],
                "currentscheduleuri": dag_run.conf['currentscheduleuri'],
                "schedulechange": dag_run.conf['schedulechange'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_132_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_132_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_132')}}"
        )

        trigger_dag_run_child_workflow_for_change_in_schedule_and_pto_1_policy_update_134 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_workflow_for_change_in_schedule_and_pto_1_policy_update_134',
            retries=0,
            trigger_dag_id=config.child_workflow_for_change_in_schedule_and_pto_1_policy_update_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri":  dag_run.conf['useruri'],
                "type": dag_run.conf['type'],
                "EEStatus":  dag_run.conf['EEStatus'],
                "EmplID_Login":  dag_run.conf['EmplID_Login'],
                "FirstName":  dag_run.conf['FirstName'],
                "LastName":  dag_run.conf['LastName'],
                "EEType":  dag_run.conf['EEType'],
                "JobCode":  dag_run.conf['JobCode'],
                "JobTitle":  dag_run.conf['JobTitle'],
                "FLSAStatus":  dag_run.conf['FLSAStatus'],
                "ServiceDate":  dag_run.conf['ServiceDate'],
                "TerminationDate":  dag_run.conf['TerminationDate'],
                "Agency_Org2":  dag_run.conf['Agency_Org2'],
                "AgencyDescription":  dag_run.conf['AgencyDescription'],
                "SupervisorID":  dag_run.conf['SupervisorID'],
                "SupervisorName":  dag_run.conf['SupervisorName'],
                "E_Mail":  dag_run.conf['E_Mail'],
                "HourlyRate":  dag_run.conf['HourlyRate'],
                "WeeklySTDHrs":  dag_run.conf['WeeklySTDHrs'],
                "Schedule":  dag_run.conf['Schedule'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "ProfitCenter":  dag_run.conf['ProfitCenter'],
                "ProfitCenterDescription":  dag_run.conf['ProfitCenterDescription'],
                "CpnyCode":  dag_run.conf['CpnyCode'],
                "PayGroupCode":  dag_run.conf['PayGroupCode'],
                "PayGroup":  dag_run.conf['PayGroup'],
                "PTO_1":  dag_run.conf['PTO_1'],
                "PTO_Bereavement":  dag_run.conf['PTO_Bereavement'],
                "PTO_JuryDuty":  dag_run.conf['PTO_JuryDuty'],
                "HolidayType":  dag_run.conf['HolidayType'],
                "Illness":  dag_run.conf['Illness'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "VTO":  dag_run.conf['VTO'],
                "EmergencySick":  dag_run.conf['EmergencySick'],
                "PayRules":  dag_run.conf['PayRules'],
                "TimesheetTemplate":  dag_run.conf['TimesheetTemplate'],
                "TimeOffTemplate":  dag_run.conf['TimeOffTemplate'],
                "HolidayCalendars":  dag_run.conf['HolidayCalendars'],
                "TimeZone":  dag_run.conf['TimeZone'],
                "WorkWeek":  dag_run.conf['WorkWeek'],
                "PayrollRegional": null,
                "PayrollGrouping": null,
                "TimeAdministrator": null,
                "TimeAdministratorGrouping": null,
                "Agency_Access": null,
                "AgencyGrouping": null,
                "LocationCode_Work":  dag_run.conf['LocationCode_Work'],
                "Dept_Org4":  dag_run.conf['Dept_Org4'],
                "Dept_Org4Desc":  dag_run.conf['Dept_Org4Desc'],
                "CoreSupervisorID":  dag_run.conf['CoreSupervisorID'],
                "CoreSupervisorName":  dag_run.conf['CoreSupervisorName'],
                "LOASuspendPTOStart":  dag_run.conf['LOASuspendPTOStart'],
                "LOASuspendPTOEnd":  dag_run.conf['LOASuspendPTOEnd'],
                "previousstartdate": dag_run.conf['previousstartdate'],
                "makeuptimepto": dag_run.conf['makeuptimepto'],
                "previous_schedule": dag_run.conf['previous_schedule'],
                'yearly_reset_script_uri': rail.result('get_required_script_uri_from_timeoffbalanceevent_scripts')['yearly_reset_script_uri'],
                "currentschedule": dag_run.conf['currentschedule'],
                "currentscheduleuri": dag_run.conf['currentscheduleuri'],
                "schedulechange": dag_run.conf['schedulechange'],
                "flsa_changed": dag_run.conf['flsa_changed'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_134_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_134_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_workflow_for_change_in_schedule_and_pto_1_policy_update_134')}}"
        )

        foreach_declare_list_45_84_end = rail.EmptyOperator(
            task_id='foreach_declare_list_45_84_end',
        )

        child_dag_ids = rail.PythonOperator(
            task_id='child_dag_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('wait_for_dag_runs')] if rail.get_dag_run_var('wait_for_dag_runs') else []
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('child_dag_ids') | to_json}}"
        )

        gather_responses_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_child',
            dag_runs='{{ result("child_dag_ids") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.responses_from_child_timeout),
            flatten=True
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Add Timeoff for transfer user- Dag_Run Error - {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                'catch_and_log_error') or rail.result('gather_responses_from_child')
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> declare_variable_6

        declare_variable_6 >> declare_variable_7 >> get_required_script_uri_from_timeoffbalanceevent_scripts >> log_tenure_basedon_pto_seniority_date >> get_time_off_type_assignments_for_user_9 \
            >> log_10 >> if_log_10_present_sick_pay_h_not_eligible_anymore_12

        if_log_10_present_sick_pay_h_not_eligible_anymore_12 >> rail.Label(
            'No') >> update_variable_18
        if_log_10_present_sick_pay_h_not_eligible_anymore_12 >> rail.Label(
            'Yes') >> trigger_dag_run_child_update_sick_pay_h_policy_on_ineligibity_13 \
            >> wait_for_completion_trigger_dag_run_child_update_sick_pay_h_policy_on_ineligibity_13 >> update_variable_18

        update_variable_18 >> log_get_value_for_stop_accruals_and_previous_pto1_timeofftype_uri >> if_request_loa_stop_accruals_equals_to_yes_24

        if_request_loa_stop_accruals_equals_to_yes_24 >> rail.Label(
            'No') >> if_declare_variable_8_value_equals_to_yes_32
        if_request_loa_stop_accruals_equals_to_yes_24 >> rail.Label(
            'Yes') >> update_variable_25 >> if_log_previous_p_t_o1timeofftype_uri_23_present_27

        if_log_previous_p_t_o1timeofftype_uri_23_present_27 >> rail.Label(
            'No') >> if_log_10_present_29
        if_log_previous_p_t_o1timeofftype_uri_23_present_27 >> rail.Label(
            'Yes') >> trigger_dag_run_child_update_pto_policy_on_loa_28 >> wait_for_completion_trigger_dag_run_child_update_pto_policy_on_loa_28 \
            >> if_log_10_present_29

        if_log_10_present_29 >> rail.Label(
            'No') >> if_declare_variable_8_value_equals_to_yes_32
        if_log_10_present_29 >> rail.Label(
            'Yes') >> trigger_dag_run_child_update_sick_pay_h_policy_on_loa_30 \
            >> wait_for_completion_trigger_dag_run_child_update_sick_pay_h_policy_on_loa_30 >> if_declare_variable_8_value_equals_to_yes_32

        if_declare_variable_8_value_equals_to_yes_32 >> rail.Label(
            'No') >> if_request_loa_stop_accruals_equals_to_yes_41
        if_declare_variable_8_value_equals_to_yes_32 >> rail.Label(
            'Yes') >> if_log_previous_p_t_o1timeofftype_uri_23_present_33

        if_log_previous_p_t_o1timeofftype_uri_23_present_33 >> rail.Label(
            'No') >> if_request_loa_stop_accruals_equals_to_yes_41
        if_log_previous_p_t_o1timeofftype_uri_23_present_33 >> rail.Label('Yes') >> get_user_time_off_type_balance_summary_35 \
            >> log_balance_minutes_and_seconds_converted_to_balance_hours \
            >> trigger_dag_run_child_remove_future_time_off_policies_transfer_and_termination_40 \
            >> wait_for_completion_trigger_dag_run_child_remove_future_time_off_policies_transfer_and_termination_40 \
            >> if_request_loa_stop_accruals_equals_to_yes_41

        if_request_loa_stop_accruals_equals_to_yes_41 >> rail.Label(
            'No') >> get_all_timeoff_types_43
        if_request_loa_stop_accruals_equals_to_yes_41 >> rail.Label(
            'Yes') >> catch_and_log_error

        get_all_timeoff_types_43 >> if_first_displaytext_present_44

        if_first_displaytext_present_44 >> rail.Label(
            'No') >> catch_and_log_error
        if_first_displaytext_present_44 >> rail.Label(
            'Yes') >> get_timeoff_uri_name_list >> log_final_set_timeoff_uris_79 >> if_log_12_present_80

        if_log_12_present_80 >> rail.Label('No') >> catch_and_log_error
        if_log_12_present_80 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_81 >> assured_partners_pto_1_time_off_list_search_entries_82 >> log_pto1_timeofflist_h_83 \
            >> create_child_triggered_list >> foreach_declare_list_45_84

        foreach_declare_list_45_84 >> if_foreach_1_uri_present_85

        if_foreach_1_uri_present_85 >> rail.Label(
            'No') >> foreach_declare_list_45_84_end
        if_foreach_1_uri_present_85 >> rail.Label(
            'Yes') >> log_checkiftimeoffwaspreviouslyassigned_86 >> accumulate_list_items_87 \
            >> if_log_pto1_timeofflist_h_83_not_contains_foreach_declare_list_45_84_name

        if_log_pto1_timeofflist_h_83_not_contains_foreach_declare_list_45_84_name >> rail.Label(
            'No') >> log_p_t_o_type_97
        if_log_pto1_timeofflist_h_83_not_contains_foreach_declare_list_45_84_name >> rail.Label(
            'Yes') >> if_request_loa_return_equals_to_yes_89

        if_request_loa_return_equals_to_yes_89 >> rail.Label(
            'No') >> if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93
        if_request_loa_return_equals_to_yes_89 >> rail.Label(
            'Yes') >> if_foreach_1_name_equals_to_sickpayh_90

        if_foreach_1_name_equals_to_sickpayh_90 >> rail.Label(
            'No') >> if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93
        if_foreach_1_name_equals_to_sickpayh_90 >> rail.Label(
            'Yes') >> if_log_checkiftimeoffwaspreviouslyassigned_86_present_91

        if_log_checkiftimeoffwaspreviouslyassigned_86_present_91 >> rail.Label(
            'No') >> if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93
        if_log_checkiftimeoffwaspreviouslyassigned_86_present_91 >> rail.Label(
            'Yes') >> trigger_dag_run_child_user_sick_pay_h_policy_update_92 >> insert_dag_92_to_wait_list \
            >> if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93

        if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93 >> rail.Label(
            'No') >> foreach_declare_list_45_84_end
        if_log_checkiftimeoffwaspreviouslyassigned_86_blank_93 >> rail.Label(
            'Yes') >> if_foreach_1_name_not_equals_to_ptopayout_94

        if_foreach_1_name_not_equals_to_ptopayout_94 >> rail.Label(
            'No') >> foreach_declare_list_45_84_end
        if_foreach_1_name_not_equals_to_ptopayout_94 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_95 \
            >> insert_dag_95_to_wait_list >> foreach_declare_list_45_84_end

        log_p_t_o_type_97 >> if_request_loa_return_equals_to_yes_98

        if_request_loa_return_equals_to_yes_98 >> rail.Label(
            'No') >> if_request_pto_1_not_equals_to_declare_variable_6_value_106
        if_request_loa_return_equals_to_yes_98 >> rail.Label(
            'Yes') >> if_log_p_t_o_type_97_equals_to_type1_99

        if_log_p_t_o_type_97_equals_to_type1_99 >> rail.Label(
            'No') >> if_log_p_t_o_type_97_equals_to_type2_101
        if_log_p_t_o_type_97_equals_to_type1_99 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_100 \
            >> insert_dag_100_to_wait_list >> if_log_p_t_o_type_97_equals_to_type2_101

        if_log_p_t_o_type_97_equals_to_type2_101 >> rail.Label(
            'No') >> if_request_pto_1_not_equals_to_declare_variable_6_value_106
        if_log_p_t_o_type_97_equals_to_type2_101 >> rail.Label(
            'Yes') >> if_foreach_1_name_equals_to_keenannoncaex_102

        if_foreach_1_name_equals_to_keenannoncaex_102 >> rail.Label(
            'No') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_105 \
            >> insert_dag_105_to_wait_list >> if_request_pto_1_not_equals_to_declare_variable_6_value_106
        if_foreach_1_name_equals_to_keenannoncaex_102 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_103 \
            >> insert_dag_103_to_wait_list >> if_request_pto_1_not_equals_to_declare_variable_6_value_106

        if_request_pto_1_not_equals_to_declare_variable_6_value_106 >> rail.Label(
            'No') >> if_log_check_if_timeoff_was_not_previously_assigned_86_blank_118
        if_request_pto_1_not_equals_to_declare_variable_6_value_106 >> rail.Label(
            'Yes') >> log_minutesconvertedtohours_107 >> if_log_p_t_o_type_97_equals_to_type1_108

        if_log_p_t_o_type_97_equals_to_type1_108 >> rail.Label(
            'No') >> if_log_p_t_o_type_97_equals_to_type2_110
        if_log_p_t_o_type_97_equals_to_type1_108 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_109 \
            >> insert_dag_109_to_wait_list >> if_log_p_t_o_type_97_equals_to_type2_110

        if_log_p_t_o_type_97_equals_to_type2_110 >> rail.Label(
            'No') >> if_log_p_t_o_type_97_equals_to_sickpto_115
        if_log_p_t_o_type_97_equals_to_type2_110 >> rail.Label(
            'Yes') >> if_foreach_1_name_equals_to_keenannoncah_111

        if_foreach_1_name_equals_to_keenannoncah_111 >> rail.Label(
            'No') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_114 \
            >> insert_dag_114_to_wait_list >> if_log_p_t_o_type_97_equals_to_sickpto_115
        if_foreach_1_name_equals_to_keenannoncah_111 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_112 \
            >> insert_dag_112_to_wait_list >> if_log_p_t_o_type_97_equals_to_sickpto_115

        if_log_p_t_o_type_97_equals_to_sickpto_115 >> rail.Label(
            'No') >> foreach_declare_list_45_84_end
        if_log_p_t_o_type_97_equals_to_sickpto_115 >> rail.Label(
            'Yes') >> if_flsa_change_not_present_or_schedule_changed_or_new_eligibility

        if_flsa_change_not_present_or_schedule_changed_or_new_eligibility >> rail.Label(
            'No') >> foreach_declare_list_45_84_end
        if_flsa_change_not_present_or_schedule_changed_or_new_eligibility >> rail.Label(
            'Yes') >> trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_116

        trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_116 \
            >> insert_dag_116_to_wait_list >> foreach_declare_list_45_84_end

        if_log_check_if_timeoff_was_not_previously_assigned_86_blank_118 >> rail.Label(
            'No') >> if_request_schedulechange_equals_to_yes_130
        if_log_check_if_timeoff_was_not_previously_assigned_86_blank_118 >> rail.Label(
            'Yes') >> if_log_p_t_o_type_97_equals_to_type1_120

        if_log_p_t_o_type_97_equals_to_type1_120 >> rail.Label(
            'No') >> if_log_p_t_o_type_97_equals_to_type2_122
        if_log_p_t_o_type_97_equals_to_type1_120 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_121 \
            >> insert_dag_121_to_wait_list >> if_log_p_t_o_type_97_equals_to_type2_122

        if_log_p_t_o_type_97_equals_to_type2_122 >> rail.Label(
            'No') >> if_log_p_t_o_type_97_equals_to_sickpto_127
        if_log_p_t_o_type_97_equals_to_type2_122 >> rail.Label(
            'Yes') >> if_foreach_1_name_equals_to_keenannoncah_123

        if_foreach_1_name_equals_to_keenannoncah_123 >> rail.Label(
            'No') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_126 \
            >> insert_dag_126_to_wait_list >> if_log_p_t_o_type_97_equals_to_sickpto_127
        if_foreach_1_name_equals_to_keenannoncah_123 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_124 \
            >> insert_dag_124_to_wait_list >> if_log_p_t_o_type_97_equals_to_sickpto_127

        if_log_p_t_o_type_97_equals_to_sickpto_127 >> rail.Label(
            'No') >> foreach_declare_list_45_84_end
        if_log_p_t_o_type_97_equals_to_sickpto_127 >> rail.Label(
            'Yes') >> if_flsa_change_not_present_or_schedule_changed_or_new_eligibility_check_2

        if_flsa_change_not_present_or_schedule_changed_or_new_eligibility_check_2 >> rail.Label(
            'No') >> foreach_declare_list_45_84_end
        if_flsa_change_not_present_or_schedule_changed_or_new_eligibility_check_2 >> rail.Label(
            'Yes') >> trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_128

        trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_128 \
            >> insert_dag_128_to_wait_list >> foreach_declare_list_45_84_end

        if_request_schedulechange_equals_to_yes_130 >> rail.Label(
            'No') >> foreach_declare_list_45_84_end
        if_request_schedulechange_equals_to_yes_130 >> rail.Label(
            'Yes') >> if_foreach_1_name_equals_to_sickpayp_131

        if_foreach_1_name_equals_to_sickpayp_131 >> rail.Label(
            'No') >> trigger_dag_run_child_workflow_for_change_in_schedule_and_pto_1_policy_update_134
        if_foreach_1_name_equals_to_sickpayp_131 >> rail.Label(
            'Yes') >> trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_132

        trigger_dag_run_child_workflow_for_change_in_schedule_and_pto_1_policy_update_134 >> insert_dag_134_to_wait_list >> foreach_declare_list_45_84_end

        trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_132 \
            >> insert_dag_132_to_wait_list >> foreach_declare_list_45_84_end

        foreach_declare_list_45_84 >> foreach_declare_list_45_84_end >> child_dag_ids >> wait_for_child_dags >> gather_responses_from_child \
            >> catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
