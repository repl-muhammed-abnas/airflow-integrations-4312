from datetime import timedelta
from dateutil.relativedelta import relativedelta
import json
from airflow.models import Variable
from assuredpartnersinc.user_import_v4.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_dag_id,
        description=f'Assured Partners User Import Rehire/Transfer Time Off Proration Assignment keenan non-ca h keenan non-ca ex Child{config.instance}',
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
            no_task='log_combined_initial_tasks'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_combined_initial_tasks',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_combined_initial_tasks = rail.PythonOperator(
            task_id='log_combined_initial_tasks',
            python_callable=lambda dag_run: {
                'time_off_policy_mapper_search_entries': list(filter(lambda x: x["type"] == (dag_run.conf['timeofftypename'].replace('-H', "").replace('-EX', "").replace('H', "").replace('EX', "")).strip(), config.TO_POLICY_MAPPER)),
                'number_of_working_days_in_week': python_callable.parse_schedule_name(
                    dag_run.conf['schedulename'])['number_of_working_days_in_week'],
                'effective_date_derived': python_callable.get_effective_date_derived(dag_run)
            }
        )

        log_hoursday_5 = rail.PythonOperator(
            task_id='log_hoursday_5',
            python_callable=lambda dag_run:  float(dag_run.conf['weekly_scheduled_hours']) / float(
                rail.result('log_combined_initial_tasks')['number_of_working_days_in_week'])
        )

        get_defaultpolicyfromgloballevel_15 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_15',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        log_existing_timeoff_policy_16 = rail.RepliconServiceOperator(
            task_id='log_existing_timeoff_policy_16',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule')
        )

        get_all_scripts_time_off_balance_event_17 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_balance_event_17',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        def get_offset_to_consider_keenan_non_ca_h_keenan_non_ca_ex(dag_run):
            tenure_of_employee = float(dag_run.conf['tenure'])
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
                    5 if tenure_of_employee >= 5 and tenure_of_employee < 10 else (10 if tenure_of_employee >= 10 and tenure_of_employee < 15 else 15)))

            return offset_to_consider

        log_relevant_historical_policies_and_offset_to_consider_and_effective_date = rail.PythonOperator(
            task_id='log_relevant_historical_policies_and_offset_to_consider_and_effective_date',
            python_callable=lambda dag_run: {
                'relevant_historical_policies': python_callable.get_relevant_historical_policies(rail.result('log_existing_timeoff_policy_16'), rail.result('log_combined_initial_tasks')['effective_date_derived']),
                'offset_to_consider': get_offset_to_consider_keenan_non_ca_h_keenan_non_ca_ex(dag_run),
                'effective_date': python_callable.get_effective_date(dag_run)
            }
        )

        def get_modified_policysetschedule(default_policysetschedule, hours_per_day, combined_initial_tasks, historical_policies_and_offset_to_consider_and_effective_date, dag_run):
            modified_policysetschedule = []
            policy_added_check = False
            for item in default_policysetschedule:

                if float(item['startOffset']['offsetValue']) == float(
                        historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                    policy_added_check = True

                if float(item['startOffset']['offsetValue']) >= float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                    entitlement_based_on_offset = rail.find_first_by_attr_and_get_attr(
                        combined_initial_tasks['time_off_policy_mapper_search_entries'], 'offset', str(item['startOffset']['offsetValue']), 'entitlement')
                    entitlement_derived_in_hours = float(
                        entitlement_based_on_offset) * float(hours_per_day)

                    accrual_annual_amount_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                        default_policysetschedule, item['startOffset']['offsetValue'], 'Accrues time once per month.', 'urn:replicon:script-key:parameter:accrual-annual-amount')

                    default_accrual_annual_amount_script = json.dumps(
                        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": accrual_annual_amount_from_default_policy}})
                    new_accrual_annual_amount_script = json.dumps(
                        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": entitlement_derived_in_hours}})

                    gsub_to_get_rid_of_starting_balance = python_callable.get_timeoffbalanceeventscript_to_gsub(
                        default_policysetschedule, item['startOffset']['offsetValue'], 'Set initial balance for the first day of a policy')

                    new_carry_over = float(list(filter(lambda x: x['offset'] == str(item['startOffset']['offsetValue']), combined_initial_tasks['time_off_policy_mapper_search_entries']))[
                        0]['carryover']) * hours_per_day

                    existing_carry_over_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                        default_policysetschedule, item['startOffset']['offsetValue'], 'Reset balance once a year', 'urn:replicon:script-key:parameter:reset-balance-amount')

                    default_gsub_value_for_carry_over = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                        "number": existing_carry_over_from_default_policy}}) if existing_carry_over_from_default_policy else 'abc~'
                    new_carry_over_gsub = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                        "number": new_carry_over}}) if new_carry_over else 'abc~'

                    if (float(item['startOffset']['offsetValue']) == float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']) and dag_run.conf['type'] == 'rehire'):
                        policy_set_based_on_offset = json.loads(json.dumps(item['policySet'], ensure_ascii=False).replace(default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(
                            default_gsub_value_for_carry_over, new_carry_over_gsub).replace('"null"', '"effective"').replace(
                            '"script"', '"scriptTarget"'))

                    else:
                        policy_set_based_on_offset = json.loads(json.dumps(item['policySet'], ensure_ascii=False).replace(default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(
                            default_gsub_value_for_carry_over, new_carry_over_gsub).replace(gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace('"null"', '"effective"').replace(
                            '"script"', '"scriptTarget"'))

                    if float(item['startOffset']['offsetValue']) == float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                        modified_policysetschedule.append({
                            'description': 'Effective on - ' + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['month']) + "/" + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['day']) + "/" + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['year']),
                            'effectiveDate': historical_policies_and_offset_to_consider_and_effective_date['effective_date'],
                            'policySet': policy_set_based_on_offset
                        })

                    elif float(item['startOffset']['offsetValue']) > float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                        effective_date_with_offset = python_callable.get_split_date(python_callable.get_split_date(
                            dag_run.conf['PTOSeniorityDate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])*12), 'int') if dag_run.conf['PTOSeniorityDate'] else python_callable.get_split_date(python_callable.get_split_date(
                                dag_run.conf['startdate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])*12), 'int')
                        modified_policysetschedule.append({
                            'description': 'Effective on - ' + str(effective_date_with_offset['month']) + "/" + str(effective_date_with_offset['day']) + "/" + str(effective_date_with_offset['year']),
                            'effectiveDate': effective_date_with_offset,
                            'policySet': policy_set_based_on_offset
                        })

            return {
                'modified_policysetschedule': modified_policysetschedule,
                'policy_added_check': policy_added_check
            }

        log_modified_policysetschedule_52_93 = rail.PythonOperator(
            task_id='log_modified_policysetschedule_52_93',
            python_callable=lambda dag_run:  get_modified_policysetschedule(rail.result('get_defaultpolicyfromgloballevel_15'), rail.result(
                'log_hoursday_5'), rail.result('log_combined_initial_tasks'), rail.result('log_relevant_historical_policies_and_offset_to_consider_and_effective_date'), dag_run)
        )

        check_if_policy_not_added = rail.IfOperator(
            task_id='check_if_policy_not_added',
            test=lambda: rail.result('log_modified_policysetschedule_52_93')[
                'policy_added_check'] != True,
            yes_task='log_modified_policysetschedule_if_offset_not_equals_offset_to_consider_95_130',
            no_task='log_modified_policysetschedule_with_historical_policies_131_134'
        )

        def get_modified_policysetschedule_2(default_policysetschedule, hours_per_day, combined_initial_tasks, historical_policies_and_offset_to_consider_and_effective_date, modified_policysetschedule, dag_run):
            offset_list = []
            for item in default_policysetschedule:
                if float(item['startOffset']['offsetValue']) < float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                    offset_list.append({
                        'offset': int(item['startOffset']['offsetValue']),
                        'diff': float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']) - float(item['startOffset']['offsetValue'])
                    })

            new_offset_to_consider = rail.find_first_by_attr_and_get_attr(
                offset_list, 'diff', max(offset_list, key=lambda y: y['diff'])['diff'], 'offset') if offset_list else ''

            policyset_to_modify = next(iter(filter(
                lambda x: int(x['startOffset']['offsetValue']) == new_offset_to_consider, default_policysetschedule)), {}).get('policySet', '')

            if bool(policyset_to_modify):
                entitlement_based_on_offset = rail.find_first_by_attr_and_get_attr(
                    combined_initial_tasks['time_off_policy_mapper_search_entries'], 'offset', str(new_offset_to_consider), 'entitlement')
                entitlement_derived_in_hours = float(
                    entitlement_based_on_offset) * float(hours_per_day)

                gsub_to_get_rid_of_starting_balance = python_callable.get_timeoffbalanceeventscript_to_gsub(
                    default_policysetschedule, new_offset_to_consider, 'Set initial balance for the first day of a policy')

                accrual_annual_amount_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                    default_policysetschedule, new_offset_to_consider, 'Accrues time once per month.', 'urn:replicon:script-key:parameter:accrual-annual-amount')

                default_accrual_annual_amount_script = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": accrual_annual_amount_from_default_policy}})
                new_accrual_annual_amount_script = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": entitlement_derived_in_hours}})

                new_carry_over = float(list(filter(lambda x: x['offset'] == str(new_offset_to_consider), combined_initial_tasks['time_off_policy_mapper_search_entries']))[
                    0]['carryover']) * hours_per_day

                existing_carry_over_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                    default_policysetschedule, new_offset_to_consider, 'Reset balance once a year', 'urn:replicon:script-key:parameter:reset-balance-amount')

                default_gsub_value_for_carry_over = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                    "number": existing_carry_over_from_default_policy}}) if existing_carry_over_from_default_policy else 'abc~'
                new_carry_over_gsub = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                    "number": new_carry_over}}) if new_carry_over else 'abc~'

                if dag_run.conf['type'] == 'rehire':
                    policy_set_based_on_offset = json.loads(json.dumps(policyset_to_modify, ensure_ascii=False).replace(default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(
                        default_gsub_value_for_carry_over, new_carry_over_gsub).replace('"null"', '"effective"').replace(
                        '"script"', '"scriptTarget"'))
                elif dag_run.conf['type'] != 'rehire':
                    policy_set_based_on_offset = json.loads(json.dumps(policyset_to_modify, ensure_ascii=False).replace(default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(
                        default_gsub_value_for_carry_over, new_carry_over_gsub).replace(gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace('"null"', '"effective"').replace(
                        '"script"', '"scriptTarget"'))

                modified_policysetschedule.append({
                    'description': 'Effective on - ' + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['month']) + "/" + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['day']) + "/" + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['year']),
                    'effectiveDate': historical_policies_and_offset_to_consider_and_effective_date['effective_date'],
                    'policySet': policy_set_based_on_offset
                })

            return modified_policysetschedule

        log_modified_policysetschedule_if_offset_not_equals_offset_to_consider_95_130 = rail.PythonOperator(
            task_id='log_modified_policysetschedule_if_offset_not_equals_offset_to_consider_95_130',
            python_callable=lambda dag_run:  get_modified_policysetschedule_2(rail.result('get_defaultpolicyfromgloballevel_15'), rail.result(
                'log_hoursday_5'), rail.result('log_combined_initial_tasks'), rail.result(
                'log_relevant_historical_policies_and_offset_to_consider_and_effective_date'), rail.result(
                'log_modified_policysetschedule_52_93')['modified_policysetschedule'], dag_run)
        )

        def add_historical_policies_to_final_policysetschedule(historical_policies_and_offset_to_consider_and_effective_date, modified_policysetschedule):
            if "urn" in json.dumps(rail.result('log_relevant_historical_policies_and_offset_to_consider_and_effective_date')['relevant_historical_policies']):
                for item in historical_policies_and_offset_to_consider_and_effective_date['relevant_historical_policies']:
                    modified_policysetschedule.append({
                        'description': item['description'],
                        'effectiveDate': item['effectiveDate'],
                        'policySet': item['policySet']
                    })

            return modified_policysetschedule

        log_modified_policysetschedule_with_historical_policies_131_134 = rail.PythonOperator(
            task_id='log_modified_policysetschedule_with_historical_policies_131_134',
            python_callable=lambda:  add_historical_policies_to_final_policysetschedule(rail.result(
                'log_relevant_historical_policies_and_offset_to_consider_and_effective_date'), (rail.result(
                    'log_modified_policysetschedule_if_offset_not_equals_offset_to_consider_95_130') or rail.result(
                    'log_modified_policysetschedule_52_93')['modified_policysetschedule']))
        )

        def add_extra_policy_lines_to_modified_policysetschedule(all_timeoffbalance_event_scripts, relevant_historical_policies_and_offset_to_consider_and_effective_date, modified_policysetschedule_with_historical_policies, dag_run):
            starting_balance_script_uri = rail.find_first_by_attr_and_get_attr(
                all_timeoffbalance_event_scripts, 'displayText', 'Starting Balance Set To', 'uri')

            if dag_run.conf['type'] == 'rehire':
                new_policy_line_rehire = json.dumps({"timeOffBalanceEventScripts": [{"scriptTarget": {"uri": starting_balance_script_uri}, "additionalParameters": [
                                                    {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": "0"}}, {"keyUri": "urn:replicon:script-key:parameter:precedence", "value": {"number": "20"}}]}], "timeOffValidationScripts": []}, ensure_ascii=False)
                eff_date_rehire = python_callable.get_split_date(python_callable.dict_date_to_datetime(
                    relevant_historical_policies_and_offset_to_consider_and_effective_date['effective_date']) - timedelta(days=1), 'int')
                modified_policysetschedule_with_historical_policies.append({
                    'description': 'Effective on ' + str(eff_date_rehire['month']) + "/" + str(eff_date_rehire['day']) + "/" + str(eff_date_rehire['year']),
                    'effectiveDate': eff_date_rehire,
                    'policySet': json.loads(new_policy_line_rehire)
                })

            if dag_run.conf['type'] == 'transfer':
                new_policy_line_transfer = json.dumps({"timeOffBalanceEventScripts": [{"scriptTarget": {"uri": starting_balance_script_uri}, "additionalParameters": [{"keyUri": "urn:replicon:script-key:parameter:amount", "value": {
                    "number": dag_run.conf['previousbalance']}}, {"keyUri": "urn:replicon:script-key:parameter:precedence", "value": {"number": "20"}}]}], "timeOffValidationScripts": []}, ensure_ascii=False)
                eff_date_transfer = python_callable.get_split_date((
                    python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'no_split') - timedelta(days=1)), 'int')
                modified_policysetschedule_with_historical_policies.append({
                    'description': 'Effective on ' + str(eff_date_transfer['month']) + "/" + str(eff_date_transfer['day']) + "/" + str(eff_date_transfer['year']),
                    'effectiveDate': eff_date_transfer,
                    'policySet': json.loads(new_policy_line_transfer)
                })

            return modified_policysetschedule_with_historical_policies

        log_final_policyset_147 = rail.PythonOperator(
            task_id='log_final_policyset_147',
            python_callable=lambda dag_run: add_extra_policy_lines_to_modified_policysetschedule(rail.result("get_all_scripts_time_off_balance_event_17"), rail.result(
                "log_relevant_historical_policies_and_offset_to_consider_and_effective_date"), rail.result("log_modified_policysetschedule_with_historical_policies_131_134"), dag_run)
        )

        assign_time_offpolicy_148 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_148',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policyset_147')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Rehire Transfer Timeoff Type Proration Assignment - Keenan Non-CA H or Keenan Non-CA EX: {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or "Success"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> final_response_from_dag
        can_run_batch_task >> rail.Label(
            'No') >> log_combined_initial_tasks

        log_combined_initial_tasks >> log_hoursday_5 >> get_defaultpolicyfromgloballevel_15 >> log_existing_timeoff_policy_16 >> get_all_scripts_time_off_balance_event_17 \
            >> log_relevant_historical_policies_and_offset_to_consider_and_effective_date >> log_modified_policysetschedule_52_93 >> check_if_policy_not_added

        check_if_policy_not_added >> rail.Label(
            'No') >> log_modified_policysetschedule_with_historical_policies_131_134
        check_if_policy_not_added >> rail.Label(
            'Yes') >> log_modified_policysetschedule_if_offset_not_equals_offset_to_consider_95_130 >> log_modified_policysetschedule_with_historical_policies_131_134

        log_modified_policysetschedule_with_historical_policies_131_134 >> log_final_policyset_147 >> assign_time_offpolicy_148 >> catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
