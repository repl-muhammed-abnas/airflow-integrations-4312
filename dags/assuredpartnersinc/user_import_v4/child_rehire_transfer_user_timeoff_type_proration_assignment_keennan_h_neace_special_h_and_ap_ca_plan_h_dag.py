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
        dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_dag_id,
        description=f'Assured Partners User Import Rehire/Transfer user timeoff proration assignment keennan-h neace special-h and ap ca plan-h and AP CO PLAN Child{config.instance}',
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

        get_all_scripts_time_off_balance_event_17 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_balance_event_17',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        log_existing_timeoff_policy_21 = rail.RepliconServiceOperator(
            task_id='log_existing_timeoff_policy_21',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule')
        )

        def get_offset_to_consider_keenan_h_neace_special_h_ap_ca_plan_h(dag_run):
            tenure_of_employee = float(dag_run.conf['tenure'])
            offset_to_consider = 0

            if "Keenan" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 2 else (2 if tenure_of_employee >= 2 and tenure_of_employee < 5 else (
                    5 if tenure_of_employee >= 5 and tenure_of_employee < 10 else 10))

            if "Neace-Special" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 2 else (2 if tenure_of_employee >= 2 and tenure_of_employee < 3 else (
                    3 if tenure_of_employee >= 3 and tenure_of_employee < 4 else (4 if tenure_of_employee >= 4 and tenure_of_employee < 5 else (
                        5 if tenure_of_employee >= 5 and tenure_of_employee < 6 else (6 if tenure_of_employee >= 6 and tenure_of_employee < 7 else (
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

            if "AP CO Plan" in dag_run.conf['timeofftypename']:
                offset_to_consider = 0 if tenure_of_employee < 5 else (
                    5 if tenure_of_employee >= 5 and tenure_of_employee < 10 else (
                        10 if tenure_of_employee >= 10 and tenure_of_employee < 15 else 15))

            return offset_to_consider

        log_relevant_historical_policies_and_offset_to_consider_and_effective_date = rail.PythonOperator(
            task_id='log_relevant_historical_policies_and_offset_to_consider_and_effective_date',
            python_callable=lambda dag_run: {
                'relevant_historical_policies': python_callable.get_relevant_historical_policies(rail.result('log_existing_timeoff_policy_21'), rail.result(
                    'log_combined_initial_tasks')['effective_date_derived']),
                'offset_to_consider': get_offset_to_consider_keenan_h_neace_special_h_ap_ca_plan_h(dag_run),
                'effective_date': python_callable.get_effective_date(dag_run)
            }
        )

        def get_max_diff_offset(default_policysetschedule, historical_policies_and_offset_to_consider_and_effective_date):
            offset_list = []
            equal_offset_available_check = False
            for item in default_policysetschedule:
                if float(item['startOffset']['offsetValue']) == float(
                        historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                    equal_offset_available_check = True

                if float(item['startOffset']['offsetValue']) < float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                    offset_list.append({
                        'offset': int(item['startOffset']['offsetValue']),
                        'diff': float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']) - float(item['startOffset']['offsetValue'])
                    })

            offset_with_max_diff = rail.find_first_by_attr_and_get_attr(
                offset_list, 'diff', max(offset_list, key=lambda y: y['diff'])['diff'], 'offset') if offset_list else ''

            return {
                'equal_offset_available_check': equal_offset_available_check,
                'offset_with_max_diff': offset_with_max_diff
            }

        log_offset_with_max_diff = rail.PythonOperator(
            task_id='log_offset_with_max_diff',
            python_callable=lambda: get_max_diff_offset(rail.result('get_defaultpolicyfromgloballevel_15'), rail.result(
                'log_relevant_historical_policies_and_offset_to_consider_and_effective_date'))
        )

        def get_modified_policysetschedule(default_policysetschedule, historical_policies_and_offset_to_consider_and_effective_date, max_diff_and_max_diff_offset, combined_initial_tasks, hours_per_day, dag_run):
            modified_policysetschedule = []
            for item in default_policysetschedule:
                if float(item['startOffset']['offsetValue']) >= float(
                    historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']) or float(
                        historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider'] if max_diff_and_max_diff_offset['equal_offset_available_check'] == True else max_diff_and_max_diff_offset['offset_with_max_diff']) == int(item['startOffset']['offsetValue']):

                    entitlement_based_on_offset = rail.find_first_by_attr_and_get_attr(
                        combined_initial_tasks['time_off_policy_mapper_search_entries'], 'offset', str(item['startOffset']['offsetValue']), 'entitlement')
                    entitlement_derived_in_hours = float(
                        entitlement_based_on_offset) * float(hours_per_day)

                    gsub_to_get_rid_of_starting_balance = python_callable.get_timeoffbalanceeventscript_to_gsub(
                        default_policysetschedule, item['startOffset']['offsetValue'], 'Set initial balance for the first day of a policy')

                    accrual_annual_amount_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                        default_policysetschedule, item['startOffset']['offsetValue'], 'Accrues time once per month.', 'urn:replicon:script-key:parameter:accrual-annual-amount')

                    default_accrual_annual_amount_script = json.dumps(
                        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": accrual_annual_amount_from_default_policy}})
                    new_accrual_annual_amount_script = json.dumps(
                        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": entitlement_derived_in_hours}})

                    new_max_balance_for_offset = float(list(filter(lambda x: x['offset'] == str(item['startOffset']['offsetValue']), combined_initial_tasks['time_off_policy_mapper_search_entries']))[
                        0]['carryover']) * hours_per_day

                    existing_max_balance = python_callable.get_required_value_from_policy_set_schedule(
                        default_policysetschedule, item['startOffset']['offsetValue'], 'Set maximum balance cap', 'urn:replicon:script-key:parameter:daily-maximum-balance-amount')

                    default_gsub_value_for_max_balance = json.dumps({"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {
                        "number": existing_max_balance}}) if existing_max_balance else 'abc~'
                    new_max_balance_to_gsub = json.dumps({"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {
                        "number": new_max_balance_for_offset}}) if new_max_balance_for_offset else 'abc~'

                    if (float(item['startOffset']['offsetValue']) == float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']) and dag_run.conf['type'] == 'rehire'):
                        policy_set_based_on_offset = json.loads(json.dumps(item['policySet'], ensure_ascii=False).replace(default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(
                            default_gsub_value_for_max_balance, new_max_balance_to_gsub).replace('"null"', '"effective"').replace(
                            '"script"', '"scriptTarget"'))

                    else:
                        policy_set_based_on_offset = json.loads(json.dumps(item['policySet'], ensure_ascii=False).replace(default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(
                            default_gsub_value_for_max_balance, new_max_balance_to_gsub).replace(gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace('"null"', '"effective"').replace(
                            '"script"', '"scriptTarget"'))

                    if dag_run.conf['timeofftypename'] != 'DRA Plan-EX' and dag_run.conf['timeofftypename'] != 'DRA Plan-H':
                        if bool('' if rail.result('log_offset_with_max_diff')['equal_offset_available_check'] == True else rail.result('log_offset_with_max_diff')['offset_with_max_diff']) and int(rail.result(
                                'log_offset_with_max_diff')['offset_with_max_diff']) == int(item['startOffset']['offsetValue']):
                            modified_policysetschedule.append({
                                'description': 'Effective on - ' + str(
                                    historical_policies_and_offset_to_consider_and_effective_date['effective_date']['month']) + "/" + str(
                                        historical_policies_and_offset_to_consider_and_effective_date['effective_date']['day']) + "/" + str(
                                            historical_policies_and_offset_to_consider_and_effective_date['effective_date']['year']),
                                'effectiveDate': historical_policies_and_offset_to_consider_and_effective_date['effective_date'],
                                'policySet': policy_set_based_on_offset
                            })
                        if float(item['startOffset']['offsetValue']) == float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                            modified_policysetschedule.append({
                                'description': 'Effective on - ' + str(
                                    historical_policies_and_offset_to_consider_and_effective_date['effective_date']['month']) + "/" + str(
                                        historical_policies_and_offset_to_consider_and_effective_date['effective_date']['day']) + "/" + str(
                                            historical_policies_and_offset_to_consider_and_effective_date['effective_date']['year']),
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

                    elif dag_run.conf['timeofftypename'] == 'DRA Plan-EX' or dag_run.conf['timeofftypename'] == 'DRA Plan-H':
                        if str(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']) == '2':
                            if bool('' if rail.result('log_offset_with_max_diff')['equal_offset_available_check'] == True else rail.result(
                                'log_offset_with_max_diff')['offset_with_max_diff']) and int(rail.result('log_offset_with_max_diff')['offset_with_max_diff']) == int(
                                    item['startOffset']['offsetValue']):
                                modified_policysetschedule.append({
                                    'description': 'Effective on - ' + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['month']) + "/" + str(
                                        historical_policies_and_offset_to_consider_and_effective_date['effective_date']['day']) + "/" + str(
                                            historical_policies_and_offset_to_consider_and_effective_date['effective_date']['year']),
                                    'effectiveDate': historical_policies_and_offset_to_consider_and_effective_date['effective_date'],
                                    'policySet': policy_set_based_on_offset
                                })

                            if float(item['startOffset']['offsetValue']) == float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                                modified_policysetschedule.append({
                                    'description': 'Effective on - ' + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['month']) + "/" + str(
                                        historical_policies_and_offset_to_consider_and_effective_date['effective_date']['day']) + "/" + str(
                                            historical_policies_and_offset_to_consider_and_effective_date['effective_date']['year']),
                                    'effectiveDate': historical_policies_and_offset_to_consider_and_effective_date['effective_date'],
                                    'policySet': policy_set_based_on_offset
                                })

                            else:
                                if item['startOffset']['offsetUnitUri'] == 'urn:replicon:time-off-policy-offset-unit:months':
                                    effective_date_with_offset_in_months = python_callable.get_split_date(python_callable.get_split_date(
                                        dag_run.conf['PTOSeniorityDate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])), 'int') if dag_run.conf['PTOSeniorityDate'] else python_callable.get_split_date(python_callable.get_split_date(
                                            dag_run.conf['startdate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])), 'int')
                                    modified_policysetschedule.append({
                                        'description': 'Effective on - ' + str(effective_date_with_offset_in_months['month']) + "/" + str(
                                            effective_date_with_offset_in_months['day']) + "/" + str(effective_date_with_offset_in_months['year']),
                                        'effectiveDate': effective_date_with_offset_in_months,
                                        'policySet': policy_set_based_on_offset
                                    })

                                elif item['startOffset']['offsetUnitUri'] != 'urn:replicon:time-off-policy-offset-unit:months':
                                    effective_date_with_offset_in_years = python_callable.get_split_date(python_callable.get_split_date(
                                        dag_run.conf['PTOSeniorityDate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])*12), 'int') if dag_run.conf['PTOSeniorityDate'] else python_callable.get_split_date(python_callable.get_split_date(
                                            dag_run.conf['startdate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])*12), 'int')
                                    modified_policysetschedule.append({
                                        'description': 'Effective on - ' + str(effective_date_with_offset_in_years['month']) + "/" + str(effective_date_with_offset_in_years['day']) + "/" + str(effective_date_with_offset_in_years['year']),
                                        'effectiveDate': effective_date_with_offset_in_years,
                                        'policySet': policy_set_based_on_offset
                                    })

                        if str(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']) != '2':
                            if bool('' if rail.result('log_offset_with_max_diff')['equal_offset_available_check'] == True else rail.result('log_offset_with_max_diff')['offset_with_max_diff']) and int(rail.result('log_offset_with_max_diff')['offset_with_max_diff']) == int(item['startOffset']['offsetValue']):
                                modified_policysetschedule.append({
                                    'description': 'Effective on - ' + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['month']) + "/" + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['day']) + "/" + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['year']),
                                    'effectiveDate': historical_policies_and_offset_to_consider_and_effective_date['effective_date'],
                                    'policySet': policy_set_based_on_offset
                                })

                            if float(item['startOffset']['offsetValue']) == float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                                modified_policysetschedule.append({
                                    'description': 'Effective on - ' + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['month']) + "/" + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['day']) + "/" + str(historical_policies_and_offset_to_consider_and_effective_date['effective_date']['year']),
                                    'effectiveDate': historical_policies_and_offset_to_consider_and_effective_date['effective_date'],
                                    'policySet': policy_set_based_on_offset
                                })

                            elif float(item['startOffset']['offsetValue']) > float(historical_policies_and_offset_to_consider_and_effective_date['offset_to_consider']):
                                if item['startOffset']['offsetUnitUri'] == 'urn:replicon:time-off-policy-offset-unit:months':
                                    effective_date_with_offset_in_months = python_callable.get_split_date(python_callable.get_split_date(
                                        dag_run.conf['PTOSeniorityDate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])), 'int') if dag_run.conf['PTOSeniorityDate'] else python_callable.get_split_date(python_callable.get_split_date(
                                            dag_run.conf['startdate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])), 'int')
                                    modified_policysetschedule.append({
                                        'description': 'Effective on - ' + str(effective_date_with_offset_in_months['month']) + "/" + str(effective_date_with_offset_in_months['day']) + "/" + str(effective_date_with_offset_in_months['year']),
                                        'effectiveDate': effective_date_with_offset_in_months,
                                        'policySet': policy_set_based_on_offset
                                    })

                                elif item['startOffset']['offsetUnitUri'] != 'urn:replicon:time-off-policy-offset-unit:months':
                                    effective_date_with_offset_in_years = python_callable.get_split_date(python_callable.get_split_date(
                                        dag_run.conf['PTOSeniorityDate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])*12), 'int') if dag_run.conf['PTOSeniorityDate'] else python_callable.get_split_date(python_callable.get_split_date(
                                            dag_run.conf['startdate'], 'no_split') + relativedelta(months=int(item['startOffset']['offsetValue'])*12), 'int')
                                    modified_policysetschedule.append({
                                        'description': 'Effective on - ' + str(effective_date_with_offset_in_years['month']) + "/" + str(effective_date_with_offset_in_years['day']) + "/" + str(effective_date_with_offset_in_years['year']),
                                        'effectiveDate': effective_date_with_offset_in_years,
                                        'policySet': policy_set_based_on_offset
                                    })

            return modified_policysetschedule

        log_modified_policysetschedule_58_123 = rail.PythonOperator(
            task_id='log_modified_policysetschedule_58_123',
            python_callable=lambda dag_run: get_modified_policysetschedule(rail.result('get_defaultpolicyfromgloballevel_15'), rail.result(
                'log_relevant_historical_policies_and_offset_to_consider_and_effective_date'), rail.result('log_offset_with_max_diff'), rail.result(
                    'log_combined_initial_tasks'), rail.result('log_hoursday_5'), dag_run)
        )

        def add_historical_policies_to_final_policysetschedule(historical_policies_and_offset_to_consider_and_effective_date, modified_policysetschedule):
            if "urn" in json.dumps(historical_policies_and_offset_to_consider_and_effective_date['relevant_historical_policies']):
                for item in historical_policies_and_offset_to_consider_and_effective_date['relevant_historical_policies']:
                    modified_policysetschedule.append({
                        'description': item['description'],
                        'effectiveDate': item['effectiveDate'],
                        'policySet': item['policySet']
                    })

            return modified_policysetschedule

        log_modified_policysetschedule_with_historical_policies_124 = rail.PythonOperator(
            task_id='log_modified_policysetschedule_with_historical_policies_124',
            python_callable=lambda:  add_historical_policies_to_final_policysetschedule(rail.result(
                'log_relevant_historical_policies_and_offset_to_consider_and_effective_date'), rail.result(
                    'log_modified_policysetschedule_58_123'))
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

        log_final_policyset_140 = rail.PythonOperator(
            task_id='log_final_policyset_140',
            python_callable=lambda dag_run: add_extra_policy_lines_to_modified_policysetschedule(rail.result("get_all_scripts_time_off_balance_event_17"), rail.result(
                "log_relevant_historical_policies_and_offset_to_consider_and_effective_date"), rail.result("log_modified_policysetschedule_with_historical_policies_124"), dag_run)
        )

        assign_time_offpolicy_146 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_146',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policyset_140')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Rehire Transfer Timeoff Type Proration Assignment - keennan H and Neace special H and AP CA plan H : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or "Success"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> log_combined_initial_tasks

        log_combined_initial_tasks >> log_hoursday_5 >> get_defaultpolicyfromgloballevel_15 >> get_all_scripts_time_off_balance_event_17 \
            >> log_existing_timeoff_policy_21 >> log_relevant_historical_policies_and_offset_to_consider_and_effective_date >> log_offset_with_max_diff \
            >> log_modified_policysetschedule_58_123 >> log_modified_policysetschedule_with_historical_policies_124 >> log_final_policyset_140 \
            >> assign_time_offpolicy_146 >> catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
