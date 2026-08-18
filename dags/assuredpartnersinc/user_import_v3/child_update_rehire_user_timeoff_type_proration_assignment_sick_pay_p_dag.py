from datetime import timedelta, datetime
import json
from airflow.models import Variable
from assuredpartnersinc.user_import_v3.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_dag_id,
        description=f'Assured Partners User Import Update/Rehire user timeoff type proration assignment Sick Pay-P Child {config.instance}',
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
            no_task='log_existing_timeoff_policy_sick_pay_p'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_existing_timeoff_policy_sick_pay_p',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_existing_timeoff_policy_sick_pay_p = rail.RepliconServiceOperator(
            task_id='log_existing_timeoff_policy_sick_pay_p',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule')
        )

        if_existing_policy_present_and_no_rehire_no_schedule_change = rail.IfOperator(
            task_id='if_existing_policy_present_and_no_rehire_no_schedule_change',
            test=lambda dag_run: rail.result('log_existing_timeoff_policy_sick_pay_p') and (
                dag_run.conf['schedulechange'] != 'yes') and dag_run.conf['type'] != 'rehire',
            yes_task='catch_and_log_error',
            no_task='log_users_starting_month'
        )

        def get_users_starting_month(dag_run):
            if dag_run.conf['type'] == 'rehire':
                return datetime.strptime(dag_run.conf['servicedate'], config.DATE_DEFAULT_FORMAT).strftime("%B")
            return datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).strftime("%B")

        log_users_starting_month = rail.PythonOperator(
            task_id='log_users_starting_month',
            python_callable=get_users_starting_month
        )

        assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_7 = rail.PythonOperator(
            task_id='assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_7',
            python_callable=lambda dag_run:  next(iter(filter(
                lambda x: x["type"] == dag_run.conf['timeofftypename'] and x["startingmonth"] == rail.result('log_users_starting_month'), config.TO_SICK_PAY_P_MAPPER)), {})
        )

        number_of_working_days_in_week = rail.PythonOperator(
            task_id='number_of_working_days_in_week',
            python_callable=lambda dag_run: python_callable.parse_schedule_name(
                dag_run.conf['schedulename'])['number_of_working_days_in_week']
        )

        log_hoursday_9 = rail.PythonOperator(
            task_id='log_hoursday_9',
            python_callable=lambda dag_run:  float(
                dag_run.conf['weekly_scheduled_hours']) / float(rail.result('number_of_working_days_in_week'))
        )

        get_current_office_schedule_details = rail.RepliconServiceOperator(
            task_id='get_current_office_schedule_details',
            endpoint="/services/OfficeScheduleService1.svc/GetOfficeScheduleDetails",
            data={
                "officeScheduleUri": "{{dag_run.conf.currentscheduleuri}}"
            },
            data_handler=lambda response, dag_run: python_callable.check_not_simplepattern_or_0_hours(
                response, dag_run)
        )

        check_not_simplepattern_or_0_hours_per_week = rail.IfOperator(
            task_id='check_not_simplepattern_or_0_hours_per_week',
            test=lambda: rail.result(
                'get_current_office_schedule_details')['hours_per_week'] == 0,
            yes_task='log_exception_sick_pay_p_not_updated',
            no_task='get_number_of_working_days_in_week_for_currentschedule_and_hours_per_day'
        )

        log_exception_sick_pay_p_not_updated = rail.PythonOperator(
            task_id='log_exception_sick_pay_p_not_updated',
            python_callable=lambda: "Exception :Sick Pay-P not updated since " + rail.result(
                'get_current_office_schedule_details')['details']
        )

        def get_working_days_in_week_and_hours_per_day(dag_run):
            number_of_working_days_in_week_for_currentschedule = python_callable.parse_schedule_name(
                dag_run.conf['currentschedule'])['number_of_working_days_in_week']
            hours_per_day_for_currentschedule = float(rail.result(
                'get_current_office_schedule_details')['hours_per_week']/number_of_working_days_in_week_for_currentschedule)
            return {
                "number_of_working_days_in_week_for_currentschedule": number_of_working_days_in_week_for_currentschedule,
                "hours_per_day_for_currentschedule": hours_per_day_for_currentschedule
            }

        get_number_of_working_days_in_week_for_currentschedule_and_hours_per_day = rail.PythonOperator(
            task_id='get_number_of_working_days_in_week_for_currentschedule_and_hours_per_day',
            python_callable=get_working_days_in_week_and_hours_per_day
        )

        log_derived_carry_over_value_for_limitation_rule_yearly_reset_and_derived_starting_balance_from_mapper_newschedule_10_12 = rail.PythonOperator(
            task_id='log_derived_carry_over_value_for_limitation_rule_yearly_reset_and_derived_starting_balance_from_mapper_newschedule_10_12',
            python_callable=lambda: {
                'derived_carry_over_value': float(rail.result('log_hoursday_9')) * float(rail.result(
                    "assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_7")['carry_over']),
                'derived_starting_balance': float(rail.result('log_hoursday_9')) * float(rail.result(
                    "assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_7")['startingbalance'])
            }
        )

        if_existing_policyset_for_sick_pay_p = rail.IfOperator(
            task_id='if_existing_policyset_for_sick_pay_p',
            test=lambda: rail.result('log_existing_timeoff_policy_sick_pay_p'),
            yes_task='if_schedule_change_is_yes',
            no_task='dummy_derived_starting_balance_to_apply'
        )

        if_schedule_change_is_yes = rail.IfOperator(
            task_id='if_schedule_change_is_yes',
            test="{{ dag_run.conf.schedulechange == 'yes' }}",
            yes_task='if_user_not_rehired',
            no_task='dummy_derived_starting_balance_to_apply'
        )

        if_user_not_rehired = rail.IfOperator(
            task_id='if_user_not_rehired',
            test="{{ dag_run.conf.type != 'rehire' }}",
            yes_task='get_current_balance_for_timeoff',
            no_task='dummy_derived_starting_balance_to_apply'
        )

        get_current_balance_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_current_balance_for_timeoff',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "asOfDate": python_callable.get_split_date(datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT) - timedelta(days=1), 'datetime')
            },
            data_handler=lambda res: res['timeRemaining'] if res else 0
        )

        def get_timeoff_balance_starting_balance_schedulechange_scenario(current_balance_for_timeoff, hours_per_day_currentschedule, hours_per_day_newschedule):
            remaining_balance_in_days = float(
                current_balance_for_timeoff)/float(hours_per_day_currentschedule)
            starting_balance_to_apply = remaining_balance_in_days * hours_per_day_newschedule
            return {
                'remaining_balance_in_days': remaining_balance_in_days,
                'starting_balance_to_apply': starting_balance_to_apply
            }

        remaining_balance_in_days_and_starting_balance_to_apply_schedule_change_scenario = rail.PythonOperator(
            task_id='remaining_balance_in_days_and_starting_balance_to_apply_schedule_change_scenario',
            python_callable=lambda: get_timeoff_balance_starting_balance_schedulechange_scenario(rail.result('get_current_balance_for_timeoff'), rail.result(
                'get_number_of_working_days_in_week_for_currentschedule_and_hours_per_day')['hours_per_day_for_currentschedule'], rail.result('log_hoursday_9'))
        )

        dummy_derived_starting_balance_to_apply = rail.EmptyOperator(
            task_id='dummy_derived_starting_balance_to_apply',
        )

        derived_starting_balance_to_apply = rail.PythonOperator(
            task_id='derived_starting_balance_to_apply',
            python_callable=lambda: rail.result(
                'remaining_balance_in_days_and_starting_balance_to_apply_schedule_change_scenario')['starting_balance_to_apply'] if rail.result(
                'remaining_balance_in_days_and_starting_balance_to_apply_schedule_change_scenario') else rail.result(
                'log_derived_carry_over_value_for_limitation_rule_yearly_reset_and_derived_starting_balance_from_mapper_newschedule_10_12')['derived_starting_balance']
        )

        get_defaultpolicyfromgloballevel_15 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_15',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{dag_run.conf.timeoffuri}}"
            }
        )

        get_all_scripts_time_off_balance_event_17 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_balance_event_17',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        log_relevant_historical_policies = rail.PythonOperator(
            task_id='log_relevant_historical_policies',
            python_callable=lambda dag_run: python_callable.get_relevant_historical_policies(rail.result(
                'log_existing_timeoff_policy_sick_pay_p'), python_callable.get_split_date(dag_run.conf['integration_run_date'], 'int'))
        )

        def add_historical_policies_to_policysets_list(relevant_historical_policies):
            policyset_list = []
            if "urn" in json.dumps(relevant_historical_policies):
                for item in relevant_historical_policies:
                    policyset_list.append({
                        'description': item['description'],
                        'effectiveDate': item['effectiveDate'],
                        'policySet': item['policySet']
                    })
            return policyset_list

        log_add_historical_policies_to_policyset_39 = rail.PythonOperator(
            task_id='log_add_historical_policies_to_policyset_39',
            python_callable=lambda:  add_historical_policies_to_policysets_list(
                rail.result('log_relevant_historical_policies'))
        )

        def get_final_policyset(default_policysetschedule, policyset_list, derived_carry_over_value, derived_starting_balance, dag_run):
            new_policy_lines = []
            for item in default_policysetschedule:
                new_policy_lines.append({
                    'description': "Effective on - " + dag_run.conf['startdate'],
                    'effectiveDate': python_callable.get_split_date(dag_run.conf['startdate'], 'int'),
                    'policySet': item['policySet']
                })
            existing_carry_over_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                default_policysetschedule, "0", 'Reset balance once a year', 'urn:replicon:script-key:parameter:reset-balance-amount')

            default_gsub_value_for_carry_over = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                "number": existing_carry_over_from_default_policy}})

            new_carry_over_gsub = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                "number": derived_carry_over_value}})

            gsub_to_get_rid_of_previous_starting_balance = python_callable.get_timeoffbalanceeventscript_to_gsub(
                default_policysetschedule, 0, 'Set initial balance for the first day of a policy')

            script_with_derived_value_of_starting_balance = python_callable.starting_balance_script_with_required_starting_balance(json.loads(
                gsub_to_get_rid_of_previous_starting_balance), derived_starting_balance)

            new_policyset = json.loads(json.dumps(new_policy_lines, ensure_ascii=False).replace(default_gsub_value_for_carry_over, new_carry_over_gsub).replace(
                gsub_to_get_rid_of_previous_starting_balance, script_with_derived_value_of_starting_balance))

            policyset_list.extend(new_policyset)

            final_policyset = json.loads(json.dumps(policyset_list, ensure_ascii=False).replace('"null"', '"effective"').replace(
                '"script"', '"scriptTarget"'))

            return final_policyset

        log_final_policyset_to_assign_56 = rail.PythonOperator(
            task_id='log_final_policyset_to_assign_56',
            python_callable=lambda dag_run:  get_final_policyset(rail.result('get_defaultpolicyfromgloballevel_15'), rail.result(
                'log_add_historical_policies_to_policyset_39'), rail.result(
                    'log_derived_carry_over_value_for_limitation_rule_yearly_reset_and_derived_starting_balance_from_mapper_newschedule_10_12')[
                        'derived_carry_over_value'], rail.result(
                            'derived_starting_balance_to_apply'), dag_run)
        )

        assign_time_offpolicy_57 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_57',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policyset_to_assign_56')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Timeoff Assignment - Sick Pay-P : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_and_log_error') if rail.result('catch_and_log_error') else (rail.result(
                'log_exception_sick_pay_p_not_updated') if rail.result('log_exception_sick_pay_p_not_updated') else "Success")
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> log_existing_timeoff_policy_sick_pay_p

        log_existing_timeoff_policy_sick_pay_p >> if_existing_policy_present_and_no_rehire_no_schedule_change

        if_existing_policy_present_and_no_rehire_no_schedule_change >> rail.Label(
            'Yes') >> catch_and_log_error
        if_existing_policy_present_and_no_rehire_no_schedule_change >> rail.Label(
            'No') >> log_users_starting_month

        log_users_starting_month >> assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_7 \
            >> number_of_working_days_in_week >> log_hoursday_9 >> get_current_office_schedule_details >> check_not_simplepattern_or_0_hours_per_week

        check_not_simplepattern_or_0_hours_per_week >> rail.Label(
            'No') >> log_exception_sick_pay_p_not_updated >> catch_and_log_error
        check_not_simplepattern_or_0_hours_per_week >> rail.Label(
            'Yes') >> get_number_of_working_days_in_week_for_currentschedule_and_hours_per_day

        get_number_of_working_days_in_week_for_currentschedule_and_hours_per_day \
            >> log_derived_carry_over_value_for_limitation_rule_yearly_reset_and_derived_starting_balance_from_mapper_newschedule_10_12 \
            >> if_existing_policyset_for_sick_pay_p

        if_existing_policyset_for_sick_pay_p >> rail.Label(
            'No') >> dummy_derived_starting_balance_to_apply
        if_existing_policyset_for_sick_pay_p >> rail.Label(
            'Yes') >> if_schedule_change_is_yes

        if_schedule_change_is_yes >> rail.Label(
            'No') >> dummy_derived_starting_balance_to_apply
        if_schedule_change_is_yes >> rail.Label(
            'Yes') >> if_user_not_rehired

        if_user_not_rehired >> rail.Label(
            'No') >> dummy_derived_starting_balance_to_apply
        if_user_not_rehired >> rail.Label(
            'Yes') >> get_current_balance_for_timeoff >> remaining_balance_in_days_and_starting_balance_to_apply_schedule_change_scenario \
            >> dummy_derived_starting_balance_to_apply

        dummy_derived_starting_balance_to_apply >> derived_starting_balance_to_apply >> get_defaultpolicyfromgloballevel_15 >> get_all_scripts_time_off_balance_event_17

        get_all_scripts_time_off_balance_event_17 >> log_relevant_historical_policies >> log_add_historical_policies_to_policyset_39 \
            >> log_final_policyset_to_assign_56 >> assign_time_offpolicy_57 >> catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
