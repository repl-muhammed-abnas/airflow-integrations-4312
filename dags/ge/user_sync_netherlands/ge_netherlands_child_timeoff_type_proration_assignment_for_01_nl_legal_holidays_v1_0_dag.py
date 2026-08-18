
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_0_{config.instance}',
        description=f'GE Netherlands_Child Timeoff type Proration Assignment for 	01. NL_Legal Holidays v1.0 {config.instance}',
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
            no_task='get_default_time_off_type_policy_schedule_for_user_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_default_time_off_type_policy_schedule_for_user_4',
            end_task='catch_95_95_95',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_default_time_off_type_policy_schedule_for_user_4 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_4',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                }
            }
        )

        if_effectivedate_day_present_6 = rail.IfOperator(
            task_id='if_effectivedate_day_present_6',
            test='''{{ result('get_default_time_off_type_policy_schedule_for_user_4') | length > 0 and result('get_default_time_off_type_policy_schedule_for_user_4')[0].effectiveDate.day | is_truthy }}''',
            yes_task="log_gettheaccrualbalancesetup_7",
            no_task="catch_95_95_95",
        )

        log_gettheaccrualbalancesetup_7 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_7',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "Yearly Accrual with Expiry", "additionalParameters")
        )

        parse_json_8 = rail.PythonOperator(
            task_id='parse_json_8',
            python_callable=lambda: rail.result(
                'log_gettheaccrualbalancesetup_7')
        )

        log_gettheaccrualbalance_9 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_9',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('parse_json_8'),
                                                                         'keyUri',
                                                                         'urn:replicon:script-key:parameter:accrual-annual-amount',
                                                                         'value.number')
        )

        log_existing_accrual_10 = rail.PythonOperator(
            task_id='log_existing_accrual_10',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_gettheaccrualbalance_9')}},
                ensure_ascii=False)
        )

        log_gettheaccrualcarryover_11 = rail.PythonOperator(
            task_id='log_gettheaccrualcarryover_11',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('parse_json_8'),
                                                                          'keyUri',
                                                                          'urn:replicon:script-key:parameter:expire-after',
                                                                          'value.number')
        )

        log_existing_accrual_carry_over_12 = rail.PythonOperator(
            task_id='log_existing_accrual_carry_over_12',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:expire-after", "value": {"number": rail.result('log_gettheaccrualcarryover_11')}},
                ensure_ascii=False)
        )

        log_required_accrual_carry_over_13 = rail.PythonOperator(
            task_id='log_required_accrual_carry_over_13',
            python_callable=lambda dag_run:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:expire-after", "value": {"number": dag_run.conf['carryover']}},
                ensure_ascii=False)
        )

        log_gettheaccrualcarryoverunit_14 = rail.PythonOperator(
            task_id='log_gettheaccrualcarryoverunit_14',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_8'), 'keyUri', 'urn:replicon:script-key:parameter:expire-after-unit', 'value.uri')
        )

        log_existing_accrual_carry_over_unit_15 = rail.PythonOperator(
            task_id='log_existing_accrual_carry_over_unit_15',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:expire-after-unit", "value": {"uri": rail.result('log_gettheaccrualcarryoverunit_14')}},
                ensure_ascii=False)
        )

        log_required_accrual_carry_over_unit_16 = rail.PythonOperator(
            task_id='log_required_accrual_carry_over_unit_16',
            python_callable=lambda dag_run:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:expire-after-unit", "value": {"uri": "urn:replicon:time-off-expire-after-unit:" + dag_run.conf['units'].lower()}},
                ensure_ascii=False)
        )

        def get_starting_balance_script():
            starting_balance_script = list(filter(lambda x: x['script']['name'] == "Starting Balance Set To", rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts']))
            return json.dumps(starting_balance_script[0], ensure_ascii=False) if starting_balance_script else []

        log_getthestartingbalancesetup_17 = rail.PythonOperator(
            task_id='log_getthestartingbalancesetup_17',
            python_callable=get_starting_balance_script
        )

        parse_json_19 = rail.PythonOperator(
            task_id='parse_json_19',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "Starting Balance Set To", "additionalParameters")
        )

        log_getthestartingbalance_20 = rail.PythonOperator(
            task_id='log_getthestartingbalance_20',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('parse_json_19'),
                                                                          'keyUri',
                                                                          'urn:replicon:script-key:parameter:amount',
                                                                          'value.number', 0.0)
        )

        log_existing_starting_balance_21 = rail.PythonOperator(
            task_id='log_existing_starting_balance_21',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": rail.result('log_getthestartingbalance_20')}},
                ensure_ascii=False)
        )

        def get_number_of_days_proration(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = start_date + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            return (start_of_year.timestamp() - start_date.timestamp()) / 86400

        log_required_numberofdaysforprorationcalculation_22 = rail.PythonOperator(
            task_id='log_required_numberofdaysforprorationcalculation_22',
            python_callable=get_number_of_days_proration
        )

        log_required_accrual_23 = rail.PythonOperator(
            task_id='log_required_accrual_23',
            python_callable=lambda dag_run:  round((float(
                dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result('log_gettheaccrualbalance_9')))
        )

        log_required_accrual_json_24 = rail.PythonOperator(
            task_id='log_required_accrual_json_24',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_23')}},
                ensure_ascii=False)
        )

        if_request_type_equals_to_add_25 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_25',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="log_required_starting_balance_26",
            no_task="if_request_type_equals_to_update_30",
        )

        def get_required_starting_balance(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = start_date + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            end_of_last_year = start_of_year + timedelta(days=-1)
            day_of_year = int(end_of_last_year.strftime('%j'))
            return round(float(((float(dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result('log_gettheaccrualbalance_9'))) / day_of_year) * float(rail.result('log_required_numberofdaysforprorationcalculation_22')))

        log_required_starting_balance_26 = rail.PythonOperator(
            task_id='log_required_starting_balance_26',
            python_callable=get_required_starting_balance
        )

        log_required_starting_balance_json_27 = rail.PythonOperator(
            task_id='log_required_starting_balance_json_27',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": rail.result('log_required_starting_balance_26')}},
                ensure_ascii=False)
        )

        log_timeoff_policy_28 = rail.PythonOperator(
            task_id='log_timeoff_policy_28',
            python_callable=lambda:  json.loads(json.dumps(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4'), ensure_ascii=False).replace(
                rail.result('log_existing_starting_balance_21'), rail.result('log_required_starting_balance_json_27')).replace(
                rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_json_24')).replace(
                rail.result('log_existing_accrual_carry_over_12'), rail.result('log_required_accrual_carry_over_13')).replace(
                rail.result('log_existing_accrual_carry_over_unit_15'), rail.result('log_required_accrual_carry_over_unit_16')).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_29 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_29',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_28')
            }
        )

        if_request_type_equals_to_update_30 = rail.IfOperator(
            task_id='if_request_type_equals_to_update_30',
            test='''{{ dag_run.conf.type == 'Update' }}''',
            yes_task="log_required_accrual_31",
            no_task="catch_95_95_95",
        )

        def get_required_accrual(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = start_date + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            end_of_last_year = start_of_year + timedelta(days=-1)
            day_of_year = int(end_of_last_year.strftime('%j'))
            return round((((float(dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result('log_gettheaccrualbalance_9'))) / day_of_year) * float(rail.result('log_required_numberofdaysforprorationcalculation_22')))

        log_required_accrual_31 = rail.PythonOperator(
            task_id='log_required_accrual_31',
            python_callable=get_required_accrual
        )

        log_required_accrual_32 = rail.PythonOperator(
            task_id='log_required_accrual_32',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_31')}},
                ensure_ascii=False)
        )

        declare_list_33 = rail.SetVariableOperator(
            task_id='declare_list_33',
            append=False,
            name='timeoffpolicy',
            value=[]
        )

        get_user_time_off_type_policy_summary_34 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_34',
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
                'get_user_time_off_type_policy_summary_34')
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

        log_existing_policy_summary_50 = rail.PythonOperator(
            task_id='log_existing_policy_summary_50',
            python_callable=get_existing_to_policy_summary
        )

        def get_policy_to_consider():
            policy_to_consider_info = list(filter(lambda x: get_datetime_obj(x['effectiveDate']).strftime('%d/%m/%Y') == rail.result('log_existing_policy_summary_50')[
                                           'current_effective_date'], rail.result('log_existing_policy_summary_50')['existing_to_policy_list']))
            return policy_to_consider_info[0]['policySet'] if policy_to_consider_info else []

        log_effective_policyto_consider_50 = rail.PythonOperator(
            task_id='log_effective_policyto_consider_50',
            python_callable=get_policy_to_consider
        )

        parse_json_51 = rail.PythonOperator(
            task_id='parse_json_51',
            python_callable=lambda: rail.result(
                'log_effective_policyto_consider_50')
        )

        def get_starting_balance_script_52():
            starting_balance_script = list(filter(lambda x: x['script']['name'] == "Starting Balance Set To", rail.result(
                'parse_json_51')['timeOffBalanceEventScripts']))
            return json.dumps(starting_balance_script[0], ensure_ascii=False) if starting_balance_script else []

        log_gettheexistingstartingbalancesetup_52 = rail.PythonOperator(
            task_id='log_gettheexistingstartingbalancesetup_52',
            python_callable=get_starting_balance_script_52
        )

        log_gettheexistingaccrualsetup_53 = rail.PythonOperator(
            task_id='log_gettheexistingaccrualsetup_53',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_51')['timeOffBalanceEventScripts'],
                'script.name', "Yearly Accrual", "additionalParameters")
        )

        log_gettheaccrualbalance_55 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_55',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'log_gettheexistingaccrualsetup_53'), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')
        )

        log_existing_accrual_56 = rail.PythonOperator(
            task_id='log_existing_accrual_56',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_gettheaccrualbalance_55')}},
                ensure_ascii=False)
        )

        def get_required_accrual_57(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            effective_date_consider = datetime.strptime(rail.result(
                'log_existing_policy_summary_50')['effective_date_to_consider'], "%d/%m/%Y")
            return round((float(rail.result('log_gettheaccrualbalance_55')) / 365) * ((start_date.timestamp() - effective_date_consider.timestamp()) / 86400))

        log_required_accrual_57 = rail.PythonOperator(
            task_id='log_required_accrual_57',
            python_callable=get_required_accrual_57
        )

        log_required_accrual_58 = rail.PythonOperator(
            task_id='log_required_accrual_58',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_57')}},
                ensure_ascii=False)
        )

        log_existing_accrual_month_59 = rail.PythonOperator(
            task_id='log_existing_accrual_month_59',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'log_gettheexistingaccrualsetup_53'), 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-month', 'value.uri')
        )

        log_existing_accrual_month_60 = rail.PythonOperator(
            task_id='log_existing_accrual_month_60',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": rail.result('log_existing_accrual_month_59')}},
                ensure_ascii=False)
        )

        log_required_accrual_month_61 = rail.PythonOperator(
            task_id='log_required_accrual_month_61',
            python_callable=lambda:  "urn:replicon:month:" + datetime.strptime(rail.result(
                'log_existing_policy_summary_50')['effective_date_to_consider'], "%d/%m/%Y").strftime('%B').lower()
        )

        log_required_accrual_month_62 = rail.PythonOperator(
            task_id='log_required_accrual_month_62',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": rail.result('log_required_accrual_month_61')}},
                ensure_ascii=False)
        )

        log_existing_accrual_date_63 = rail.PythonOperator(
            task_id='log_existing_accrual_date_63',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'log_gettheexistingaccrualsetup_53'), 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-day-of-month', 'value.uri')
        )

        log_existing_accrual_date_64 = rail.PythonOperator(
            task_id='log_existing_accrual_date_64',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": rail.result('log_existing_accrual_date_63')}},
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

        log_required_accrual_date_65 = rail.PythonOperator(
            task_id='log_required_accrual_date_65',
            python_callable=lambda: get_day_option(
                rail.result('log_existing_policy_summary_50')['effective_date_to_consider'])
        )

        log_required_accrual_date_66 = rail.PythonOperator(
            task_id='log_required_accrual_date_66',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": rail.result('log_required_accrual_date_65')}},
                ensure_ascii=False)
        )

        log_policy_set_67 = rail.PythonOperator(
            task_id='log_policy_set_67',
            python_callable=lambda:  json.loads(json.dumps(rail.result('parse_json_51')['policySet'], ensure_ascii=False).replace(rail.result('log_existing_accrual_56'), rail.result('log_required_accrual_58')).replace(", "+rail.result('log_getthestartingbalancesetup_17'), "").replace(rail.result(
                'log_existing_accrual_month_60'), rail.result('log_required_accrual_month_62')).replace(rail.result('log_existing_accrual_date_64'), rail.result('log_required_accrual_date_66')).replace(", "+rail.result('log_gettheexistingstartingbalancesetup_52'), '').replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        parse_json_68 = rail.PythonOperator(
            task_id='parse_json_68',
            python_callable=lambda: rail.result('log_policy_set_67')
        )

        def add_policy_to_policy():
            policy_to_consider = rail.result('log_existing_policy_summary_50')[
                'to_policies_to_assign'] if rail.result('log_existing_policy_summary_50') else []
            if policy_to_consider:
                for policy in policy_to_consider:
                    policy['policySet'] = json.loads(json.dumps(policy['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"'))
            effective_date_to_consider = datetime.strptime(rail.result(
                'log_existing_policy_summary_50')['effective_date_to_consider'], "%d/%m/%Y")
            policy_to_consider.append({
                "description": "Effective on " + rail.result('log_existing_policy_summary_50')['effective_date_to_consider'],
                "effectiveDate": {
                    "day": effective_date_to_consider.day,
                    "month": effective_date_to_consider.month,
                    "year": effective_date_to_consider.year,
                },
                "policySet": json.loads(json.dumps(rail.result('parse_json_68'), ensure_ascii=False).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
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
                derived_policy_set = json.loads(json.dumps(policy['policySet'], ensure_ascii=False).replace(existing_accrual_obj, rail.result('log_required_accrual_32')).replace(existing_accrual_month_obj, required_accrual_month_obj).replace(existing_accrual_date_obj, required_accrual_date_obj).replace(", "+rail.result(
                    'log_getthestartingbalancesetup_17'), '').replace(rail.result('log_existing_accrual_carry_over_12'), rail.result('log_required_accrual_carry_over_13')).replace(rail.result('log_existing_accrual_carry_over_unit_15'), rail.result('log_required_accrual_carry_over_unit_16')).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))

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
            global_first_policy = json.loads(json.dumps(globalpolicy[0]['policySet'], ensure_ascii=False).replace(rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_json_24')).replace(", "+rail.result('log_getthestartingbalancesetup_17'), '').replace(rail.result(
                'log_existing_accrual_carry_over_12'), rail.result('log_required_accrual_carry_over_13')).replace(rail.result('log_existing_accrual_carry_over_unit_15'), rail.result('log_required_accrual_carry_over_unit_16')).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
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
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_95_95_95
        can_run_batch_task >> rail.Label(
            'No') >> get_default_time_off_type_policy_schedule_for_user_4 >> if_effectivedate_day_present_6
        if_effectivedate_day_present_6 >> rail.Label('Yes') >> log_gettheaccrualbalancesetup_7 >> parse_json_8 >> \
            log_gettheaccrualbalance_9 >> log_existing_accrual_10 >> log_gettheaccrualcarryover_11 >> \
            log_existing_accrual_carry_over_12 >> log_required_accrual_carry_over_13 >> log_gettheaccrualcarryoverunit_14 >> \
            log_existing_accrual_carry_over_unit_15 >> log_required_accrual_carry_over_unit_16 >> log_getthestartingbalancesetup_17 >> \
            parse_json_19 >> log_getthestartingbalance_20 >> \
            log_existing_starting_balance_21 >> log_required_numberofdaysforprorationcalculation_22 >> \
            log_required_accrual_23 >> log_required_accrual_json_24 >> if_request_type_equals_to_add_25
        if_request_type_equals_to_add_25 >> rail.Label(
            'Yes') >> log_required_starting_balance_26 >> log_required_starting_balance_json_27 >> \
            log_timeoff_policy_28 >> put_user_time_off_account_policy_set_schedule_29 >> if_request_type_equals_to_update_30
        if_request_type_equals_to_add_25 >> rail.Label(
            'No') >> if_request_type_equals_to_update_30
        if_request_type_equals_to_update_30 >> rail.Label(
            'No') >> catch_95_95_95
        if_request_type_equals_to_update_30 >> rail.Label(
            'Yes') >> log_required_accrual_31 >> log_required_accrual_32 >> declare_list_33 >> get_user_time_off_type_policy_summary_34 >> \
            log_existing_policy_summary_50 >> log_effective_policyto_consider_50 >> parse_json_51 >> log_gettheexistingstartingbalancesetup_52 >> log_gettheexistingaccrualsetup_53 >> \
            log_gettheaccrualbalance_55 >> log_existing_accrual_56 >> log_required_accrual_57 >> log_required_accrual_58 >> \
            log_existing_accrual_month_59 >> log_existing_accrual_month_60 >> log_required_accrual_month_61 >> \
            log_required_accrual_month_62 >> log_existing_accrual_date_63 >> log_existing_accrual_date_64 >> \
            log_required_accrual_date_65 >> log_required_accrual_date_66 >> log_policy_set_67 >> parse_json_68 >> add_policy_69 >> \
            get_default_time_off_policy_set_schedule_for_time_off_type_70 >> log_effective_dateto_consider_71 >> log_policytoassign_93 >> \
            put_user_time_off_account_policy_set_schedule_94 >> catch_95_95_95
        if_effectivedate_day_present_6 >> rail.Label(
            'No') >> catch_95_95_95 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
