from datetime import timedelta, datetime
import json
from airflow.models import Variable
from assuredpartnersinc.user_import_v3.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_update_pto_policy_on_loa_dag_id,
        description=f'Assured Partners User Import Update PTO policy on LOA Child {config.instance}',
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
            no_task='get_enabled_time_off_types_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_enabled_time_off_types_2',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_enabled_time_off_types_2 = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types_2',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        get_defaultpolicyfromgloballevel_5 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_5',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        get_user_time_off_type_policy_summary_6 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_6',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        log_relevant_historical_policies = rail.PythonOperator(
            task_id='log_relevant_historical_policies',
            python_callable=lambda dag_run: python_callable.get_relevant_historical_policies(rail.result(
                'get_user_time_off_type_policy_summary_6'), python_callable.get_split_date(dag_run.conf['loastartdate'], 'int'))
        )

        log_add_historical_policies_to_policyset_list_26 = rail.PythonOperator(
            task_id='log_add_historical_policies_to_policyset_list_26',
            python_callable=lambda:  python_callable.add_historical_policies_to_policysets_list(
                rail.result('log_relevant_historical_policies'))
        )

        log_combined_tasks_27_30 = rail.PythonOperator(
            task_id='log_combined_tasks_27_30',
            python_callable=lambda dag_run: {
                # category will not be used further but kept for reference ; timeofftypename will be compared against the PTO mapper timeoff names as per type
                'category_of_time_off_type': (dag_run.conf['timeofftypename'].replace('-H', "").replace('-EX', "").replace('H', "").replace('EX', "")).strip(),
                'time_off_policy_mapper_search_entries': list(filter(lambda x: x["type"] == (dag_run.conf['timeofftypename'].replace('-H', "").replace('-EX', "").replace('H', "").replace('EX', "")).strip(), config.TO_POLICY_MAPPER)),
                'number_of_working_days_in_week': python_callable.parse_schedule_name(
                    dag_run.conf['schedulename'])['number_of_working_days_in_week']
            }
        )

        log_hoursday = rail.PythonOperator(
            task_id='log_hoursday',
            python_callable=lambda dag_run:  float(dag_run.conf['weekly_scheduled_hours']) / float(
                rail.result('log_combined_tasks_27_30')['number_of_working_days_in_week'])
        )

        if_derived_timeoff_type_catagory_matches_appto_ahmplanh_apptonr_keenannonca_seattleplan_34 = rail.IfOperator(
            task_id='if_derived_timeoff_type_catagory_matches_appto_ahmplanh_apptonr_keenannonca_seattleplan_34',
            test=lambda dag_run: dag_run.conf['timeofftypename'] in [item['time_off_type_name'] for item in config.TO_PTO1_MAPPER if item['type'] == 'Type 1'],
            yes_task="log_add_policyset_to_policyset_list_46",
            no_task="if_derived_timeoff_type_catagory_matches_apcaplanh_keennan_draplan_keenanpto_47",
        )

        def add_policyset_to_policyset_list_based_on_offset_to_consider(hours_per_day, combined_initial_tasks, yearly_reset_script_uri, policyset_list, dag_run):
            tenure_of_employee = abs((python_callable.get_split_date(
                dag_run.conf['startdate'], 'no_split') - datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).date()).days / 365)
            offset_to_consider = 0

            if "Keenan Non" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 2 else (2 if tenure_of_employee >= 2 and tenure_of_employee < 5 else (
                    5 if tenure_of_employee >= 5 and tenure_of_employee < 10 else 10))

            if "AP PTO" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 2 else (
                    2 if tenure_of_employee >= 2 and tenure_of_employee < 5 else (
                        5 if tenure_of_employee >= 5 and tenure_of_employee < 10 else (
                            10 if tenure_of_employee >= 10 and tenure_of_employee < 15 else 15)))

            if "AHM Plan" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 10 else (
                    10 if tenure_of_employee >= 10 and tenure_of_employee < 15 else 15)

            if "Seattle Plan" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 2 else (2 if tenure_of_employee >= 2 and tenure_of_employee < 5 else (
                    5 if tenure_of_employee >= 5 and tenure_of_employee < 10 else (10 if tenure_of_employee < 15 else 15)))

            if "AP CO Plan" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 5 else (
                    5 if tenure_of_employee >= 5 and tenure_of_employee < 10 else (
                        10 if tenure_of_employee >= 10 and tenure_of_employee < 15 else 15))

            new_carry_over = float(list(filter(lambda x: x['offset'] == str(offset_to_consider), combined_initial_tasks['time_off_policy_mapper_search_entries']))[
                0]['carryover']) * hours_per_day

            policyset_to_add_in_policyset_list = json.loads(json.dumps({"timeOffBalanceEventScripts": [{"additionalParameters": [{"keyUri": "urn:replicon:script-key:parameter:periodic-reset-option", "value": {"uri": "urn:replicon:time-off-policy-reset-option:carry-over-previous-balance-with-limit"}}, {"keyUri": "urn:replicon:script-key:parameter:precedence", "value": {"number": 10.0}}, {"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                                                            "number": new_carry_over}}, {"keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month", "value": {"uri": "urn:replicon:monthly-frequency-start-day-option:1st"}}, {"keyUri": "urn:replicon:script-key:parameter:reset-on-month", "value": {"uri": "urn:replicon:month:january"}}], "scriptTarget": {"description": "Reset balance once a year", "name": "Yearly Reset", "uri": yearly_reset_script_uri}}], "timeOffValidationScripts": []}))

            policyset_list.append({
                'description': "Effective on - " + dag_run.conf['loastartdate'],
                'effectiveDate': python_callable.get_split_date(dag_run.conf['loastartdate'], 'int'),
                'policySet': policyset_to_add_in_policyset_list
            })

            return policyset_list

        log_add_policyset_to_policyset_list_46 = rail.PythonOperator(
            task_id='log_add_policyset_to_policyset_list_46',
            python_callable=lambda dag_run: add_policyset_to_policyset_list_based_on_offset_to_consider(rail.result("log_hoursday"), rail.result(
                "log_combined_tasks_27_30"), dag_run.conf['yearly_reset_script_uri'], rail.result("log_add_historical_policies_to_policyset_list_26"), dag_run),
        )

        if_derived_timeoff_type_catagory_matches_apcaplanh_keennan_draplan_keenanpto_47 = rail.IfOperator(
            task_id='if_derived_timeoff_type_catagory_matches_apcaplanh_keennan_draplan_keenanpto_47',
            test=lambda dag_run: dag_run.conf['timeofftypename'] in [item['time_off_type_name'] for item in config.TO_PTO1_MAPPER if item['type'] == 'Type 2'],
            yes_task="log_add_policyset_to_policyset_list_59",
            no_task="final_policyset_to_assign",
        )

        def add_policyset_2_to_policyset_list_based_on_offset_to_consider(hours_per_day, combined_initial_tasks, max_balance_limit_script_uri, policyset_list, dag_run):
            tenure_of_employee = abs((python_callable.get_split_date(
                dag_run.conf['startdate'], 'no_split') - datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).date()).days / 365)
            offset_to_consider = 0

            if "Keenan" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 2 else (2 if tenure_of_employee >= 2 and tenure_of_employee < 5 else (
                    5 if tenure_of_employee >= 5 and tenure_of_employee < 10 else 10))

            if "Neace-Special" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 2 else (
                    2 if tenure_of_employee >= 2 and tenure_of_employee < 3 else (
                        3 if tenure_of_employee >= 3 and tenure_of_employee < 4 else (
                            4 if tenure_of_employee >= 4 and tenure_of_employee < 5 else (
                                5 if tenure_of_employee >= 5 and tenure_of_employee < 6 else (
                                    6 if tenure_of_employee >= 6 and tenure_of_employee < 7 else (
                                        7 if tenure_of_employee >= 7 and tenure_of_employee < 8 else 8))))))

            if "AP CA Plan" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 2 else (
                    2 if tenure_of_employee >= 2 and tenure_of_employee < 5 else (
                        5 if tenure_of_employee >= 5 and tenure_of_employee < 10 else (
                            10 if tenure_of_employee >= 10 and tenure_of_employee < 15 else 15)))

            if "DRA Plan" in dag_run.conf['timeofftypename']:
                offset_to_consider = 2 if tenure_of_employee < 0.16 else (
                    1 if tenure_of_employee >= 0.16 and tenure_of_employee < 3 else (
                        3 if tenure_of_employee >= 3 and tenure_of_employee < 6 else (
                            6 if tenure_of_employee >= 6 and tenure_of_employee < 10 else (
                                10 if tenure_of_employee >= 10 and tenure_of_employee < 14 else (
                                    14 if tenure_of_employee >= 14 and tenure_of_employee < 18 else 18)))))

            max_balance_limit = float(list(filter(lambda x: x['offset'] == str(offset_to_consider), combined_initial_tasks['time_off_policy_mapper_search_entries']))[
                0]['carryover']) * hours_per_day

            policyset_to_add_in_policyset_list = json.loads(json.dumps({"timeOffBalanceEventScripts": [{"additionalParameters": [{"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {"number": max_balance_limit}}, {
                                                            "keyUri": "urn:replicon:script-key:parameter:precedence", "value": {"number": 10000.0}}], "scriptTarget": {"description": "Set maximum balance cap", "name": "Max Balance Limit", "uri": max_balance_limit_script_uri}}], "timeOffValidationScripts": []}))

            policyset_list.append({
                'description': "Effective on - " + dag_run.conf['loastartdate'],
                'effectiveDate': python_callable.get_split_date(dag_run.conf['loastartdate'], 'int'),
                'policySet': policyset_to_add_in_policyset_list
            })

            return policyset_list

        log_add_policyset_to_policyset_list_59 = rail.PythonOperator(
            task_id='log_add_policyset_to_policyset_list_59',
            python_callable=lambda dag_run: add_policyset_2_to_policyset_list_based_on_offset_to_consider(rail.result("log_hoursday"), rail.result(
                "log_combined_tasks_27_30"), dag_run.conf['max_balance_limit_script_uri'], rail.result("log_add_historical_policies_to_policyset_list_26"), dag_run),
        )

        final_policyset_to_assign = rail.PythonOperator(
            task_id='final_policyset_to_assign',
            python_callable=lambda: rail.result("log_add_policyset_to_policyset_list_59") or rail.result(
                "log_add_policyset_to_policyset_list_46") or rail.result("log_add_historical_policies_to_policyset_list_26")
        )

        if_final_policyset_list_contains_urn_62 = rail.IfOperator(
            task_id='if_final_policyset_list_contains_urn_62',
            test=lambda: 'urn' in json.dumps(
                rail.result('final_policyset_to_assign')),
            yes_task="assign_time_offpolicy_63",
            no_task="catch_and_log_error",
        )

        assign_time_offpolicy_63 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_63',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('final_policyset_to_assign')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Update PTO policy on LOA : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or "Success"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> get_enabled_time_off_types_2

        get_enabled_time_off_types_2 >> get_defaultpolicyfromgloballevel_5 >> get_user_time_off_type_policy_summary_6 >> log_relevant_historical_policies \
            >> log_add_historical_policies_to_policyset_list_26 >> log_combined_tasks_27_30 >> log_hoursday \
            >> if_derived_timeoff_type_catagory_matches_appto_ahmplanh_apptonr_keenannonca_seattleplan_34

        if_derived_timeoff_type_catagory_matches_appto_ahmplanh_apptonr_keenannonca_seattleplan_34 >> rail.Label(
            'No') >> if_derived_timeoff_type_catagory_matches_apcaplanh_keennan_draplan_keenanpto_47
        if_derived_timeoff_type_catagory_matches_appto_ahmplanh_apptonr_keenannonca_seattleplan_34 >> rail.Label(
            'Yes') >> log_add_policyset_to_policyset_list_46 >> if_derived_timeoff_type_catagory_matches_apcaplanh_keennan_draplan_keenanpto_47

        if_derived_timeoff_type_catagory_matches_apcaplanh_keennan_draplan_keenanpto_47 >> rail.Label(
            'No') >> final_policyset_to_assign
        if_derived_timeoff_type_catagory_matches_apcaplanh_keennan_draplan_keenanpto_47 >> rail.Label(
            'Yes') >> log_add_policyset_to_policyset_list_59 >> final_policyset_to_assign

        final_policyset_to_assign >> if_final_policyset_list_contains_urn_62

        if_final_policyset_list_contains_urn_62 >> rail.Label(
            'No') >> catch_and_log_error
        if_final_policyset_list_contains_urn_62 >> rail.Label(
            'Yes') >> assign_time_offpolicy_63 >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
