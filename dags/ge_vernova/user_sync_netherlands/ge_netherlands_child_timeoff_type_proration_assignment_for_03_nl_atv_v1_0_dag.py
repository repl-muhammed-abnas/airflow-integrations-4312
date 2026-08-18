
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from ge.user_sync_netherlands.netherlands_timeoff_mapper import netherlands_timeoff_mapper
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_user_sync_ge_netherlands_child_timeoff_type_proration_assignment_for_03_nl_atv_v1_0_{config.instance}',
        description=f'GE Netherlands_Child Timeoff type Proration Assignment for 03. NL_ATV v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='netherlands_timeoff_mapper_search_entries_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='netherlands_timeoff_mapper_search_entries_3',
            end_task='catch_95_95_95',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_netherlands_timeoff_mapper_search_entries(dag_run):
            legacy_payroll_id_payrule = dag_run.conf['legacypayrollid'] + "|" + \
                dag_run.conf['payrule'] if dag_run.conf['legacypayrollid'] == "00013105" else dag_run.conf['legacypayrollid']
            timeoff_info = list(filter(lambda x: x['timeoff_type_name'] == dag_run.conf['timeofftype']
                                and x['legacy_payroll_id_|_payrule'] == legacy_payroll_id_payrule, netherlands_timeoff_mapper))
            return timeoff_info[0] if timeoff_info else None

        netherlands_timeoff_mapper_search_entries_3 = rail.PythonOperator(
            task_id='netherlands_timeoff_mapper_search_entries_3',
            python_callable=get_netherlands_timeoff_mapper_search_entries
        )

        get_default_time_off_type_policy_schedule_for_user_5 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_5',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                }
            }
        )

        if_effectivedate_day_present_7 = rail.IfOperator(
            task_id='if_effectivedate_day_present_7',
            test='''{{ result('get_default_time_off_type_policy_schedule_for_user_5')[0].effectiveDate.day | is_truthy }}''',
            yes_task="log_required_valuetocalculate_starting_balance_8",
            no_task="catch_95_95_95",
        )

        def get_required_valuetocalculate_starting_balance():
            timeoff_info = rail.result(
                'netherlands_timeoff_mapper_search_entries_3')
            return float(timeoff_info['carryover|units'].split('|')[0]) if timeoff_info['accural_need_to_be_added_|_accrual'] == 'No' else float(timeoff_info['accural_need_to_be_added_|_accrual'].split('|')[-1])

        log_required_valuetocalculate_starting_balance_8 = rail.PythonOperator(
            task_id='log_required_valuetocalculate_starting_balance_8',
            python_callable=get_required_valuetocalculate_starting_balance
        )

        log_gettheaccrualbalancescript_9 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancescript_9',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_5')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "Yearly Accrual", "additionalParameters")
        )

        parse_json_11 = rail.PythonOperator(
            task_id='parse_json_11',
            python_callable=lambda: rail.result(
                'log_gettheaccrualbalancescript_9')
        )

        log_gettheaccrualbalance_12 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_12',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('parse_json_11'),
                                                                         'keyUri',
                                                                         'urn:replicon:script-key:parameter:accrual-annual-amount',
                                                                         'value.number')
        )

        log_existing_accrual_13 = rail.PythonOperator(
            task_id='log_existing_accrual_13',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_gettheaccrualbalance_12')}},
                ensure_ascii=False)
        )

        def get_starting_balance_script():
            starting_balance_script = list(filter(lambda x: x['script']['name'] == "Starting Balance Set To", rail.result(
                'get_default_time_off_type_policy_schedule_for_user_5')[0]['policySet']['timeOffBalanceEventScripts']))
            return json.dumps(starting_balance_script[0], ensure_ascii=False) if starting_balance_script else []

        log_getthestartingbalancescript_15 = rail.PythonOperator(
            task_id='log_getthestartingbalancescript_15',
            python_callable=get_starting_balance_script
        )

        parse_json_16 = rail.PythonOperator(
            task_id='parse_json_16',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_5')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "Starting Balance Set To", "additionalParameters")
        )

        log_getthestartingbalance_17 = rail.PythonOperator(
            task_id='log_getthestartingbalance_17',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('parse_json_16'),
                                                                          'keyUri',
                                                                          'urn:replicon:script-key:parameter:amount',
                                                                          'value.number', 0.0)
        )

        log_existing_starting_balance_18 = rail.PythonOperator(
            task_id='log_existing_starting_balance_18',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": rail.result('log_getthestartingbalance_17')}},
                ensure_ascii=False)
        )

        def get_number_of_days_proration(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = start_date + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            return (start_of_year.timestamp() - start_date.timestamp()) / 86400

        log_required_numberofdaysforprorationcalculation_19 = rail.PythonOperator(
            task_id='log_required_numberofdaysforprorationcalculation_19',
            python_callable=get_number_of_days_proration
        )

        log_required_accrual_20 = rail.PythonOperator(
            task_id='log_required_accrual_20',
            python_callable=lambda dag_run:  round((float(
                dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result('log_required_valuetocalculate_starting_balance_8')))
        )

        log_required_accrual_json_21 = rail.PythonOperator(
            task_id='log_required_accrual_json_21',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_20')}},
                ensure_ascii=False)
        )

        if_request_type_equals_to_add_22 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_22',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="log_required_starting_balance_23",
            no_task="if_request_type_equals_to_update_27",
        )

        def get_required_starting_balance(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = start_date + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            end_of_last_year = start_of_year + timedelta(days=-1)
            day_of_year = int(end_of_last_year.strftime('%j'))
            return round(float(((float(dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result('log_required_valuetocalculate_starting_balance_8'))) / day_of_year) * float(rail.result('log_required_numberofdaysforprorationcalculation_19')))

        log_required_starting_balance_23 = rail.PythonOperator(
            task_id='log_required_starting_balance_23',
            python_callable=get_required_starting_balance
        )

        log_required_starting_balance_json_24 = rail.PythonOperator(
            task_id='log_required_starting_balance_json_24',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": rail.result('log_required_starting_balance_23')}},
                ensure_ascii=False)
        )

        log_timeoff_policy_final_25 = rail.PythonOperator(
            task_id='log_timeoff_policy_final_25',
            python_callable=lambda:  json.loads(json.dumps(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_5'), ensure_ascii=False).replace(
                rail.result('log_existing_starting_balance_18'), rail.result('log_required_starting_balance_json_24')).replace(
                rail.result('log_existing_accrual_13'), rail.result('log_required_accrual_json_21')).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_26 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_26',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_final_25')
            }
        )

        if_request_type_equals_to_update_27 = rail.IfOperator(
            task_id='if_request_type_equals_to_update_27',
            test='''{{ dag_run.conf.type == 'Update' }}''',
            yes_task="log_required_accrual_28",
            no_task="catch_95_95_95",
        )

        def get_required_accrual(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = start_date + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            end_of_last_year = start_of_year + timedelta(days=-1)
            day_of_year = int(end_of_last_year.strftime('%j'))
            return round((((float(dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result('log_required_valuetocalculate_starting_balance_8'))) / day_of_year) * float(rail.result('log_required_numberofdaysforprorationcalculation_19')))

        log_required_accrual_28 = rail.PythonOperator(
            task_id='log_required_accrual_28',
            python_callable=get_required_accrual
        )

        log_required_accrual_29 = rail.PythonOperator(
            task_id='log_required_accrual_29',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_28')}},
                ensure_ascii=False)
        )

        get_user_time_off_type_policy_summary_31 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_31',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        def get_datetime_obj(effectiveDate):
            year = effectiveDate['year']
            month = effectiveDate['month']
            day = effectiveDate['day']
            return datetime.strptime(f"{day}/{month}/{year}", '%d/%m/%Y')

        def get_existing_to_policy_summary(dag_run):
            existing_to_policy_list = []
            user_time_off_type_policy_summary = rail.result(
                'get_user_time_off_type_policy_summary_31')
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_start_year = start_date.replace(month=1, day=1)
            if user_time_off_type_policy_summary['policiesByTimeOffType']:
                for time_off_type_policy_summary in user_time_off_type_policy_summary['policiesByTimeOffType']:
                    if time_off_type_policy_summary['timeOffType']['name'] == dag_run.conf['timeofftype']:
                        for policy in time_off_type_policy_summary['policySetSchedule']:
                            eff_date = get_datetime_obj(
                                policy['effectiveDate'])
                            if eff_date < start_date:
                                existing_to_policy_list.append({
                                    "description": policy['description'],
                                    "effectiveDate": {
                                        "day": eff_date.day,
                                        "month": eff_date.month,
                                        "year": eff_date.year,
                                    },
                                    "policySet": policy['policySet']
                                })
            max_date = (max(get_datetime_obj(
                x['effectiveDate']) for x in existing_to_policy_list))
            current_effective_date = max_date
            effective_date_to_consider = max_date
            if max_date < begining_start_year:
                effective_date_to_consider = begining_start_year
            to_policies_to_assign = []
            if user_time_off_type_policy_summary['policiesByTimeOffType']:
                for time_off_type_policy_summary in user_time_off_type_policy_summary['policiesByTimeOffType']:
                    if time_off_type_policy_summary['timeOffType']['displayText'] == dag_run.conf['timeofftype']:
                        for policy in time_off_type_policy_summary['policySetSchedule']:
                            eff_date = get_datetime_obj(
                                policy['effectiveDate'])
                            if eff_date < effective_date_to_consider:
                                to_policies_to_assign.append({
                                    "description": policy['description'],
                                    "effectiveDate": {
                                        "day": eff_date.day,
                                        "month": eff_date.month,
                                        "year": eff_date.year,
                                    },
                                    "policySet": policy['policySet']
                                })

            return {
                "existing_to_policy_list": existing_to_policy_list,
                "to_policies_to_assign": to_policies_to_assign,
                "effective_date_to_consider": effective_date_to_consider.strftime('%d/%m/%Y'),
                "current_effective_date": current_effective_date.strftime('%d/%m/%Y')
            }

        log_existing_policy_summary_46 = rail.PythonOperator(
            task_id='log_existing_policy_summary_46',
            python_callable=get_existing_to_policy_summary
        )

        def get_policy_to_consider():
            policy_to_consider_info = list(filter(lambda x: get_datetime_obj(x['effectiveDate']).strftime('%d/%m/%Y') == rail.result('log_existing_policy_summary_46')[
                                           'current_effective_date'], rail.result('log_existing_policy_summary_46')['existing_to_policy_list']))
            return policy_to_consider_info[0]['policySet'] if policy_to_consider_info else []

        log_effective_policyto_consider_47 = rail.PythonOperator(
            task_id='log_effective_policyto_consider_47',
            python_callable=get_policy_to_consider
        )

        parse_json_48 = rail.PythonOperator(
            task_id='parse_json_48',
            python_callable=lambda: rail.result(
                'log_effective_policyto_consider_47')
        )

        def get_starting_balance_script_49():
            starting_balance_script = list(filter(lambda x: x['script']['name'] == "Starting Balance Set To", rail.result(
                'parse_json_48')['timeOffBalanceEventScripts']))
            return json.dumps(starting_balance_script[0], ensure_ascii=False) if starting_balance_script else []

        log_gettheexistingstartingbalance_script_49 = rail.PythonOperator(
            task_id='log_gettheexistingstartingbalance_script_49',
            python_callable=get_starting_balance_script_49
        )

        log_gettheexistingaccrualbalance_script_50 = rail.PythonOperator(
            task_id='log_gettheexistingaccrualbalance_script_50',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_48')['timeOffBalanceEventScripts'],
                'script.name', "Yearly Accrual", "additionalParameters")
        )

        log_gettheexistingresetbalancescript_51 = rail.PythonOperator(
            task_id='log_gettheexistingresetbalancescript_51',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_48')['timeOffBalanceEventScripts'],
                'script.name', "Yearly Reset", "additionalParameters")
        )

        log_gettheexistingaccrualsetup_52 = rail.PythonOperator(
            task_id='log_gettheexistingaccrualsetup_52',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_48')['timeOffBalanceEventScripts'],
                'script.name', "Yearly Accrual", "additionalParameters")
        )

        parse_json_53 = rail.PythonOperator(
            task_id='parse_json_53',
            python_callable=lambda: rail.result(
                'log_gettheexistingaccrualsetup_52')
        )

        log_gettheaccrualbalance_54 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_54',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_53'), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')
        )

        log_existing_accrual_55 = rail.PythonOperator(
            task_id='log_existing_accrual_55',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_gettheaccrualbalance_54')}},
                ensure_ascii=False)
        )

        def get_required_accrual_56(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            effective_date_consider = datetime.strptime(rail.result(
                'log_existing_policy_summary_46')['effective_date_to_consider'], "%d/%m/%Y")
            return round((float(rail.result('log_gettheaccrualbalance_54')) / 365) * ((start_date.timestamp() - effective_date_consider.timestamp()) / 86400))

        log_required_accrual_56 = rail.PythonOperator(
            task_id='log_required_accrual_56',
            python_callable=get_required_accrual_56
        )

        log_required_accrual_57 = rail.PythonOperator(
            task_id='log_required_accrual_57',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_56')}},
                ensure_ascii=False)
        )

        log_existing_accrual_month_58 = rail.PythonOperator(
            task_id='log_existing_accrual_month_58',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_53'), 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-month', 'value.uri')
        )

        log_existing_accrual_month_59 = rail.PythonOperator(
            task_id='log_existing_accrual_month_59',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": rail.result('log_existing_accrual_month_58')}},
                ensure_ascii=False)
        )

        log_required_accrual_month_60 = rail.PythonOperator(
            task_id='log_required_accrual_month_60',
            python_callable=lambda:  "urn:replicon:month:" + datetime.strptime(rail.result(
                'log_existing_policy_summary_46')['effective_date_to_consider'], "%d/%m/%Y").strftime('%B').lower()
        )

        log_required_accrual_month_61 = rail.PythonOperator(
            task_id='log_required_accrual_month_61',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": rail.result('log_required_accrual_month_60')}},
                ensure_ascii=False)
        )

        log_existing_accrual_date_62 = rail.PythonOperator(
            task_id='log_existing_accrual_date_62',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_53'), 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-day-of-month', 'value.uri')
        )

        log_existing_accrual_date_63 = rail.PythonOperator(
            task_id='log_existing_accrual_date_63',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": rail.result('log_existing_accrual_date_62')}},
                ensure_ascii=False)
        )

        def get_day_option(date_str):
            accrualeffectivedate = datetime.strptime(date_str, "%d/%m/%Y")
            accrual_day_option_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                str(accrualeffectivedate.day) + "th"
            accrual_day = accrualeffectivedate.day
            if accrual_day in [1, 21, 31]:
                accrual_day_option_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                    str(accrual_day) + "st"
            if accrual_day in [2, 22]:
                accrual_day_option_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                    str(accrual_day) + "nd"
            if accrual_day in [3, 23]:
                accrual_day_option_uri = "urn:replicon:monthly-frequency-start-day-option:" + \
                    str(accrual_day) + "rd"
            return accrual_day_option_uri

        log_required_accrual_date_64 = rail.PythonOperator(
            task_id='log_required_accrual_date_64',
            python_callable=lambda: get_day_option(
                rail.result('log_existing_policy_summary_46')['effective_date_to_consider'])
        )

        log_required_accrual_date_65 = rail.PythonOperator(
            task_id='log_required_accrual_date_65',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": rail.result('log_required_accrual_date_64')}},
                ensure_ascii=False)
        )

        def get_policy_to_consider_65():
            policy_to_consider_info = list(filter(lambda x: get_datetime_obj(x['effectiveDate']).strftime('%d/%m/%Y') == rail.result('log_existing_policy_summary_46')[
                                           'current_effective_date'], rail.result('log_existing_policy_summary_46')['existing_to_policy_list']))
            return policy_to_consider_info[0]['policySet'] if policy_to_consider_info else []

        log_policy_to_consider_65 = rail.PythonOperator(
            task_id='log_policy_to_consider_65',
            python_callable=get_policy_to_consider_65
        )

        log_policy_to_dummy_65 = rail.PythonOperator(
            task_id='log_policy_to_dummy_65',
            python_callable=lambda: json.dumps(rail.result(
                'log_policy_to_consider_65'), ensure_ascii=False)
        )

        log_policy_set_final_66 = rail.PythonOperator(
            task_id='log_policy_set_final_66',
            python_callable=lambda:  json.loads(json.dumps(rail.result('log_policy_to_consider_65'), ensure_ascii=False).replace(rail.result('log_existing_accrual_55'), rail.result('log_required_accrual_57')).replace(", "+rail.result('log_getthestartingbalancescript_15'), '').replace(rail.result('log_getthestartingbalancescript_15')+",", '').replace(rail.result(
                'log_existing_accrual_month_59'), rail.result('log_required_accrual_month_61')).replace(rail.result('log_existing_accrual_date_63'), rail.result('log_required_accrual_date_65')).replace(", "+rail.result('log_gettheexistingstartingbalance_script_49'), '').replace(rail.result('log_gettheexistingstartingbalance_script_49')+",", '').replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        def timeoffbalance_validation():
            policyset = json.dumps(rail.result(
                'log_effective_policyto_consider_47'), ensure_ascii=False)
            return bool(policyset != '[{"timeOffBalanceEventScripts":[],"timeOffValidationScripts":[]}]')

        if_log_effective_policyto_consider_47_not_equals_to_timeoffbalanceeventscriptstimeoffvalidationscripts_67 = rail.IfOperator(
            task_id='if_log_effective_policyto_consider_47_not_equals_to_timeoffbalanceeventscriptstimeoffvalidationscripts_67',
            test=timeoffbalance_validation,
            yes_task="parse_json_68",
            no_task="get_default_time_off_policy_set_schedule_for_time_off_type_70",
        )

        parse_json_68 = rail.PythonOperator(
            task_id='parse_json_68',
            python_callable=lambda: rail.result('log_policy_set_final_66')
        )

        def add_policy_to_policy():
            policy_to_consider = rail.result('log_existing_policy_summary_46')[
                'to_policies_to_assign'] if rail.result('log_existing_policy_summary_46') else []
            if policy_to_consider:
                for policy in policy_to_consider:
                    policy['policySet'] = json.loads(json.dumps(policy['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"'))
            effective_date_to_consider = datetime.strptime(rail.result(
                'log_existing_policy_summary_46')['effective_date_to_consider'], "%d/%m/%Y")
            policy_to_consider.append({
                "description": "Effective on " + rail.result('log_existing_policy_summary_46')['effective_date_to_consider'],
                "effectiveDate": {
                    "day": effective_date_to_consider.day,
                    "month": effective_date_to_consider.month,
                    "year": effective_date_to_consider.year,
                },
                "policySet": rail.result('parse_json_68')
            })

            return policy_to_consider

        add_policy_69 = rail.PythonOperator(
            task_id='add_policy_69',
            python_callable=add_policy_to_policy
        )

        get_default_time_off_policy_set_schedule_for_time_off_type_70 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type_70',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        def get_effective_date_to_consider_71(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            start_date_plus_12 = start_date + relativedelta(months=12)
            begining_start_year = start_date_plus_12.replace(month=1, day=1)
            return begining_start_year.strftime('%d/%m/%Y')

        log_effective_dateto_consider_71 = rail.PythonOperator(
            task_id='log_effective_dateto_consider_71',
            python_callable=get_effective_date_to_consider_71
        )

        def get_default_policy_timeoffs(dag_run):
            global_ploicy_consider = rail.result(
                'add_policy_69') if rail.result('add_policy_69') else []
            globalpolicy = rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_70') if rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type_70') else []
            effective_to_consider = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = effective_to_consider + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            for policy in globalpolicy:
                balance_event = policy['policySet']['timeOffBalanceEventScripts']
                existing_accrual_setup = rail.find_first_by_attr_and_get_attr(
                    balance_event, 'script.name', "Yearly Accrual", "additionalParameters")
                accrual_balance = rail.find_first_by_attr_and_get_attr(
                    existing_accrual_setup, 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')
                existing_accrual_obj = json.dumps({"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {
                    "number": accrual_balance}}, ensure_ascii=False)
                existing_accrual_month = rail.find_first_by_attr_and_get_attr(
                    balance_event, 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-month', 'value.uri')
                existing_accrual_month_obj = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": existing_accrual_month}}, ensure_ascii=False)
                existing_accrual_date = rail.find_first_by_attr_and_get_attr(
                    balance_event, 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-day-of-month', 'value.uri')
                existing_accrual_date_obj = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": existing_accrual_date}}, ensure_ascii=False)

                required_accrual_month = "urn:replicon:month:" + \
                    effective_to_consider.strftime('%B').lower()
                required_accrual_month_obj = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": required_accrual_month}}, ensure_ascii=False)
                required_accrual_date = get_day_option(
                    dag_run.conf["startdate"])
                required_accrual_date_obj = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": required_accrual_date}}, ensure_ascii=False)
                derived_policy_set = json.loads(json.dumps(policy['policySet'], ensure_ascii=False).replace(existing_accrual_obj, rail.result('log_required_accrual_29')).replace(existing_accrual_month_obj, required_accrual_month_obj).replace(existing_accrual_date_obj, required_accrual_date_obj).replace(", "+rail.result(
                    'log_getthestartingbalancescript_15'), '').replace(rail.result('log_getthestartingbalancescript_15')+",", '').replace('null', '"effective"').replace('"script"', '"scriptTarget"'))

                if effective_to_consider != start_of_year:
                    global_ploicy_consider.append({
                        "description": "Effective on" + effective_to_consider.strftime('%d-%m-%Y'),
                        "effectiveDate": {
                            "day": effective_to_consider.day,
                            "month": effective_to_consider.month,
                            "year": effective_to_consider.year,
                        },
                        "policySet": derived_policy_set
                    })
            global_first_policy = json.loads(json.dumps(globalpolicy[0]['policySet'], ensure_ascii=False).replace(rail.result('log_existing_accrual_13'), rail.result(
                'log_required_accrual_json_21')).replace(", "+rail.result('log_getthestartingbalancescript_15'), '').replace(rail.result('log_getthestartingbalancescript_15')+",", '').replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
            global_ploicy_consider.append({
                "description": "Effective on" + start_of_year.strftime('%d-%m-%Y'),
                "effectiveDate": {
                    "day": start_of_year.day,
                    "month": start_of_year.month,
                    "year": start_of_year.year,
                },
                "policySet": global_first_policy
            })
            return global_ploicy_consider

        log_policytoassign_93 = rail.PythonOperator(
            task_id='log_policytoassign_93',
            python_callable=get_default_policy_timeoffs
        )

        put_user_time_off_account_policy_set_schedule_94 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_94',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_policytoassign_93')
            }
        )

        catch_95_95_95 = rail.EmptyOperator(
            task_id='catch_95_95_95',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_95_95_95
        can_run_batch_task >> rail.Label(
            'No') >> netherlands_timeoff_mapper_search_entries_3
        netherlands_timeoff_mapper_search_entries_3 >> get_default_time_off_type_policy_schedule_for_user_5 >> if_effectivedate_day_present_7
        if_effectivedate_day_present_7 >> rail.Label('Yes') >> log_required_valuetocalculate_starting_balance_8 >> \
            log_gettheaccrualbalancescript_9 >> parse_json_11 >> \
            log_gettheaccrualbalance_12 >> log_existing_accrual_13 >> \
            log_getthestartingbalancescript_15 >> parse_json_16 >> log_getthestartingbalance_17 >> log_existing_starting_balance_18 >> \
            log_required_numberofdaysforprorationcalculation_19 >> log_required_accrual_20 >> log_required_accrual_json_21 >> if_request_type_equals_to_add_22
        if_request_type_equals_to_add_22 >> rail.Label(
            'No') >> if_request_type_equals_to_update_27
        if_request_type_equals_to_add_22 >> rail.Label(
            'Yes') >> log_required_starting_balance_23 >> log_required_starting_balance_json_24 >> \
            log_timeoff_policy_final_25 >> put_user_time_off_account_policy_set_schedule_26 >> if_request_type_equals_to_update_27
        if_request_type_equals_to_update_27 >> rail.Label(
            'No') >> catch_95_95_95
        if_request_type_equals_to_update_27 >> rail.Label('Yes') >> log_required_accrual_28 >> log_required_accrual_29 >> get_user_time_off_type_policy_summary_31 >> \
            log_existing_policy_summary_46 >> log_effective_policyto_consider_47 >> parse_json_48 >> \
            log_gettheexistingstartingbalance_script_49 >> log_gettheexistingaccrualbalance_script_50 >> \
            log_gettheexistingresetbalancescript_51 >> log_gettheexistingaccrualsetup_52 >> parse_json_53 >> \
            log_gettheaccrualbalance_54 >> log_existing_accrual_55 >> log_required_accrual_56 >> \
            log_required_accrual_57 >> log_existing_accrual_month_58 >> log_existing_accrual_month_59 >> \
            log_required_accrual_month_60 >> log_required_accrual_month_61 >> log_existing_accrual_date_62 >> \
            log_existing_accrual_date_63 >> log_required_accrual_date_64 >> log_required_accrual_date_65 >> \
            log_policy_to_consider_65 >> log_policy_to_dummy_65 >> log_policy_set_final_66 >> if_log_effective_policyto_consider_47_not_equals_to_timeoffbalanceeventscriptstimeoffvalidationscripts_67
        if_log_effective_policyto_consider_47_not_equals_to_timeoffbalanceeventscriptstimeoffvalidationscripts_67 >> rail.Label(
            'Yes') >> parse_json_68 >> add_policy_69 >> get_default_time_off_policy_set_schedule_for_time_off_type_70
        if_log_effective_policyto_consider_47_not_equals_to_timeoffbalanceeventscriptstimeoffvalidationscripts_67 >> rail.Label(
            'No') >> get_default_time_off_policy_set_schedule_for_time_off_type_70 >> log_effective_dateto_consider_71 >> \
            log_policytoassign_93 >> put_user_time_off_account_policy_set_schedule_94 >> catch_95_95_95
        if_effectivedate_day_present_7 >> rail.Label(
            'No') >> catch_95_95_95 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
