
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_netherlands_child_timeoff_type_proration_assignment_for_default_v1_0_{config.instance}',
        description=f'GE Netherlands_Child Timeoff type Proration Assignment for default v1.0 {config.instance}',
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
            end_task='catch_89_89_89',
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
            no_task="catch_89_89_89",
        )

        def get_gather_accrual_balance_script():
            yearly_accrual = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "Yearly Accrual", "additionalParameters", None)
            if yearly_accrual is None:
                yearly_accrual = rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'],
                    'script.name', "Yearly Accrual with Expiry", "additionalParameters")
            return yearly_accrual

        log_gettheaccrualbalancesetup_7 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_7',
            python_callable=get_gather_accrual_balance_script
        )

        parse_json_8 = rail.PythonOperator(
            task_id='parse_json_8',
            python_callable=lambda: rail.result(
                'log_gettheaccrualbalancesetup_7')
        )

        log_gettheaccrualbalance_9 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_9',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('parse_json_8'),
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

        log_getthestartingbalancesetup_11 = rail.PythonOperator(
            task_id='log_getthestartingbalancesetup_11',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "Starting Balance Set To", "additionalParameters")
        )

        def get_starting_balance_script():
            starting_balance_script = list(filter(lambda x: x['script']['name'] == "Starting Balance Set To", rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts']))
            return json.dumps(starting_balance_script[0], ensure_ascii=False) if starting_balance_script else []

        log_getthestartingbalancescript_12 = rail.PythonOperator(
            task_id='log_getthestartingbalancescript_12',
            python_callable=get_starting_balance_script
        )

        parse_json_13 = rail.PythonOperator(
            task_id='parse_json_13',
            python_callable=lambda: rail.result(
                'log_gettheaccrualbalancesetup_7')
        )

        log_getthestartingbalance_14 = rail.PythonOperator(
            task_id='log_getthestartingbalance_14',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('parse_json_13'),
                                                                          'keyUri',
                                                                          'urn:replicon:script-key:parameter:amount',
                                                                          'value.number', 0.0)
        )

        log_existing_starting_balance_15 = rail.PythonOperator(
            task_id='log_existing_starting_balance_15',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": rail.result('log_getthestartingbalance_14')}},
                ensure_ascii=False)
        )

        def get_number_of_days_proration(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = start_date + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            return (start_of_year.timestamp() - start_date.timestamp()) / 86400

        log_required_numberofdaysforprorationcalculation_16 = rail.PythonOperator(
            task_id='log_required_numberofdaysforprorationcalculation_16',
            python_callable=get_number_of_days_proration
        )

        log_required_accrual_17 = rail.PythonOperator(
            task_id='log_required_accrual_17',
            python_callable=lambda dag_run:  round((float(
                dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result('log_gettheaccrualbalance_9')))
        )

        log_required_accrual_json_18 = rail.PythonOperator(
            task_id='log_required_accrual_json_18',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_17')}},
                ensure_ascii=False)
        )

        if_request_type_equals_to_add_19 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_19',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="log_required_starting_balance_20",
            no_task="if_request_type_equals_to_update_24",
        )

        def get_required_starting_balance(dag_run):
            starting_balance = 0.0
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year_start_date = start_date.replace(month=1, day=1)
            if start_date != begining_year_start_date:
                begining_year = start_date + relativedelta(months=12)
                start_of_year = begining_year.replace(month=1, day=1)
                end_of_last_year = start_of_year + timedelta(days=-1)
                day_of_year = int(end_of_last_year.strftime('%j'))
                starting_balance = round(((((float(dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result(
                    'log_gettheaccrualbalance_9'))) / day_of_year)) * float(rail.result('log_required_numberofdaysforprorationcalculation_16')))
            return float(starting_balance)

        log_required_starting_balance_20 = rail.PythonOperator(
            task_id='log_required_starting_balance_20',
            python_callable=get_required_starting_balance
        )

        log_required_starting_balance_json_21 = rail.PythonOperator(
            task_id='log_required_starting_balance_json_21',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": rail.result('log_required_starting_balance_20')}},
                ensure_ascii=False)
        )

        log_timeoff_policy_22 = rail.PythonOperator(
            task_id='log_timeoff_policy_22',
            python_callable=lambda:  json.loads(json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_4'), ensure_ascii=False).replace(rail.result('log_existing_starting_balance_15'), rail.result(
                'log_required_starting_balance_json_21')).replace(rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_json_18')).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_23 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_23',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_22')
            }
        )

        if_request_type_equals_to_update_24 = rail.IfOperator(
            task_id='if_request_type_equals_to_update_24',
            test='''{{ dag_run.conf.type == 'Update' }}''',
            yes_task="log_required_accrual_25",
            no_task="catch_89_89_89",
        )

        def get_required_accrual(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = start_date + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            end_of_last_year = start_of_year + timedelta(days=-1)
            day_of_year = int(end_of_last_year.strftime('%j'))
            return round((((float(dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result('log_gettheaccrualbalance_9'))) / day_of_year) * float(rail.result('log_required_numberofdaysforprorationcalculation_16')))

        log_required_accrual_25 = rail.PythonOperator(
            task_id='log_required_accrual_25',
            python_callable=get_required_accrual
        )

        log_required_accrual_26 = rail.PythonOperator(
            task_id='log_required_accrual_26',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_25')}},
                ensure_ascii=False)
        )

        get_user_time_off_type_policy_summary_28 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_28',
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
                'get_user_time_off_type_policy_summary_28')
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
                x['effectiveDate']) for x in existing_to_policy_list)) if existing_to_policy_list else None
            current_effective_date = None
            effective_date_to_consider = None
            to_policies_to_assign = []
            # pylint: disable=too-many-nested-blocks
            if max_date:
                current_effective_date = max_date
                effective_date_to_consider = max_date
                if max_date < begining_start_year:
                    effective_date_to_consider = begining_start_year
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
                "effective_date_to_consider": effective_date_to_consider.strftime('%d/%m/%Y') if effective_date_to_consider else None,
                "current_effective_date": current_effective_date.strftime('%d/%m/%Y') if current_effective_date else None,
            }

        log_existing_policy_summary_43 = rail.PythonOperator(
            task_id='log_existing_policy_summary_43',
            python_callable=get_existing_to_policy_summary
        )

        def get_policy_to_consider():
            policy_to_consider_info = list(filter(lambda x: get_datetime_obj(x['effectiveDate']).strftime('%d/%m/%Y') == rail.result('log_existing_policy_summary_43')[
                                           'current_effective_date'], rail.result('log_existing_policy_summary_43')['existing_to_policy_list'])) if rail.result('log_existing_policy_summary_43')['existing_to_policy_list'] else []
            return policy_to_consider_info[0]['policySet'] if policy_to_consider_info else []

        log_effective_policyto_consider_44 = rail.PythonOperator(
            task_id='log_effective_policyto_consider_44',
            python_callable=get_policy_to_consider
        )

        parse_json_45 = rail.PythonOperator(
            task_id='parse_json_45',
            python_callable=lambda: rail.result(
                'log_effective_policyto_consider_44')
        )

        def get_starting_balance_script_46():
            starting_balance_script = list(filter(lambda x: x['script']['name'] == "Starting Balance Set To", rail.result(
                'parse_json_45')['timeOffBalanceEventScripts'])) if rail.result('parse_json_45') else []
            return json.dumps(starting_balance_script[0], ensure_ascii=False) if starting_balance_script else []

        log_gettheexistingstartingbalancesetup_46 = rail.PythonOperator(
            task_id='log_gettheexistingstartingbalancesetup_46',
            python_callable=get_starting_balance_script_46
        )

        def get_gather_accrual_setup():
            yearly_accrual = rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_45')['timeOffBalanceEventScripts'],
                'script.name', "Yearly Accrual", "additionalParameters", None)
            if yearly_accrual is None:
                yearly_accrual = rail.find_first_by_attr_and_get_attr(rail.result(
                    'parse_json_45')['timeOffBalanceEventScripts'],
                    'script.name', "Yearly Accrual with Expiry", "additionalParameters")
            return yearly_accrual

        log_gettheexistingaccrualsetup_47 = rail.PythonOperator(
            task_id='log_gettheexistingaccrualsetup_47',
            python_callable=get_gather_accrual_setup
        )

        parse_json_48 = rail.PythonOperator(
            task_id='parse_json_48',
            python_callable=lambda: rail.result(
                'log_gettheexistingaccrualsetup_47')
        )

        log_gettheaccrualbalance_49 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_49',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_48'), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')
        )

        log_existing_accrual_50 = rail.PythonOperator(
            task_id='log_existing_accrual_50',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_gettheaccrualbalance_49')}},
                ensure_ascii=False)
        )

        def get_required_accrual_51(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            effective_date_consider = datetime.strptime(rail.result(
                'log_existing_policy_summary_43')['effective_date_to_consider'], "%d/%m/%Y")
            return round((float(rail.result('log_gettheaccrualbalance_49')) / 365) * ((start_date.timestamp() - effective_date_consider.timestamp()) / 86400))

        log_required_accrual_51 = rail.PythonOperator(
            task_id='log_required_accrual_51',
            python_callable=get_required_accrual_51
        )

        log_required_accrual_52 = rail.PythonOperator(
            task_id='log_required_accrual_52',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_51')}},
                ensure_ascii=False)
        )

        log_existing_accrual_month_53 = rail.PythonOperator(
            task_id='log_existing_accrual_month_53',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_48'), 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-month', 'value.uri')
        )

        log_existing_accrual_month_54 = rail.PythonOperator(
            task_id='log_existing_accrual_month_54',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": rail.result('log_existing_accrual_month_53')}},
                ensure_ascii=False)
        )

        log_required_accrual_month_55 = rail.PythonOperator(
            task_id='log_required_accrual_month_55',
            python_callable=lambda:  "urn:replicon:month:" + datetime.strptime(rail.result(
                'log_existing_policy_summary_43')['effective_date_to_consider'], "%d/%m/%Y").strftime('%B').lower()
        )

        log_required_accrual_month_56 = rail.PythonOperator(
            task_id='log_required_accrual_month_56',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": rail.result('log_required_accrual_month_55')}},
                ensure_ascii=False)
        )

        log_existing_accrual_date_57 = rail.PythonOperator(
            task_id='log_existing_accrual_date_57',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_48'), 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-day-of-month', 'value.uri')
        )

        log_existing_accrual_date_58 = rail.PythonOperator(
            task_id='log_existing_accrual_date_58',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": rail.result('log_existing_accrual_date_57')}},
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

        log_required_accrual_date_59 = rail.PythonOperator(
            task_id='log_required_accrual_date_59',
            python_callable=lambda:  get_day_option(
                rail.result('log_existing_policy_summary_43')['effective_date_to_consider'])
        )

        log_required_accrual_date_60 = rail.PythonOperator(
            task_id='log_required_accrual_date_60',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": rail.result('log_required_accrual_date_59')}},
                ensure_ascii=False)
        )

        def get_policy_to_consider_61():
            policy_to_consider_info = list(filter(lambda x: get_datetime_obj(x['effectiveDate']).strftime('%d/%m/%Y') == rail.result('log_existing_policy_summary_43')[
                                           'current_effective_date'], rail.result('log_existing_policy_summary_43')['existing_to_policy_list']))
            return policy_to_consider_info[0]['policySet'] if policy_to_consider_info else []

        log_policy_to_consider_61 = rail.PythonOperator(
            task_id='log_policy_to_consider_61',
            python_callable=get_policy_to_consider_61
        )

        log_policy_set_61 = rail.PythonOperator(
            task_id='log_policy_set_61',
            python_callable=lambda:  json.loads(json.dumps(rail.result('log_policy_to_consider_61'), ensure_ascii=False).replace(rail.result('log_existing_accrual_50'), rail.result('log_required_accrual_52')).replace(rail.result('log_getthestartingbalancescript_12')+",", '').replace(", "+rail.result('log_getthestartingbalancescript_12'), '').replace(rail.result(
                'log_existing_accrual_month_54'), rail.result('log_required_accrual_month_56')).replace(rail.result('log_existing_accrual_date_58'), rail.result('log_required_accrual_date_60')).replace(rail.result('log_gettheexistingstartingbalancesetup_46')+",", '').replace(", "+rail.result('log_gettheexistingstartingbalancesetup_46'), '').replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        parse_json_62 = rail.PythonOperator(
            task_id='parse_json_62',
            python_callable=lambda: rail.result('log_policy_set_61')
        )

        def add_policy_to_policy():
            policy_to_consider = rail.result('log_existing_policy_summary_43')[
                'to_policies_to_assign']
            if policy_to_consider:
                for policy in policy_to_consider:
                    policy['policySet'] = json.loads(json.dumps(policy['policySet'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"'))
            effective_date_to_consider = datetime.strptime(rail.result(
                'log_existing_policy_summary_43')['effective_date_to_consider'], "%d/%m/%Y")
            policy_to_consider.append({
                "description": "Effective on " + rail.result('log_existing_policy_summary_43')['effective_date_to_consider'],
                "effectiveDate": {
                    "day": effective_date_to_consider.day,
                    "month": effective_date_to_consider.month,
                    "year": effective_date_to_consider.year,
                },
                "policySet": rail.result('parse_json_62')
            })

            return policy_to_consider

        add_policy_63 = rail.PythonOperator(
            task_id='add_policy_63',
            python_callable=add_policy_to_policy
        )

        get_default_time_off_policy_set_schedule_for_time_off_type_64 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type_64',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        def get_effective_date_to_consider_65(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            start_date_plus_12 = start_date + relativedelta(months=12)
            begining_start_year = start_date_plus_12.replace(month=1, day=1)
            return begining_start_year.strftime('%d/%m/%Y')

        log_effective_dateto_consider_65 = rail.PythonOperator(
            task_id='log_effective_dateto_consider_65',
            python_callable=get_effective_date_to_consider_65
        )

        def get_default_policy_timeoffs(dag_run):
            global_ploicy_consider = rail.result(
                'add_policy_63') if rail.result('add_policy_63') else []
            globalpolicy = rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_64') if rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type_64') else []
            effective_to_consider = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = effective_to_consider + relativedelta(months=12)
            start_of_year = begining_year.replace(month=1, day=1)
            for policy in globalpolicy:
                balance_event = policy['policySet']['timeOffBalanceEventScripts']
                existing_accrual_setup = rail.find_first_by_attr_and_get_attr(
                    balance_event, 'script.name', "Yearly Accrual", "additionalParameters", None)
                if existing_accrual_setup is None:
                    existing_accrual_setup = rail.find_first_by_attr_and_get_attr(
                        balance_event, 'script.name', "Yearly Accrual with Expiry", "additionalParameters", None)
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
                derived_policy_set = json.loads(json.dumps(policy['policySet'], ensure_ascii=False).replace(existing_accrual_obj, rail.result('log_required_accrual_26')).replace(existing_accrual_month_obj, required_accrual_month_obj).replace(existing_accrual_date_obj, required_accrual_date_obj).replace(", "+rail.result(
                    'log_getthestartingbalancescript_12'), '').replace(rail.result('log_getthestartingbalancescript_12')+",", '').replace('null', '"effective"').replace('"script"', '"scriptTarget"'))

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
            global_first_policy = json.loads(json.dumps(globalpolicy[0]['policySet'], ensure_ascii=False).replace(rail.result('log_existing_accrual_10'), rail.result(
                'log_required_accrual_json_18')).replace(", "+rail.result('log_getthestartingbalancescript_12'), '').replace(rail.result('log_getthestartingbalancescript_12')+",", '').replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
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

        log_policytoassign_87 = rail.PythonOperator(
            task_id='log_policytoassign_87',
            python_callable=get_default_policy_timeoffs
        )

        put_user_time_off_account_policy_set_schedule_88 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_88',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_policytoassign_87')
            }
        )

        catch_89_89_89 = rail.EmptyOperator(
            task_id='catch_89_89_89',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_89_89_89
        can_run_batch_task >> rail.Label(
            'No') >> get_default_time_off_type_policy_schedule_for_user_4 >> if_effectivedate_day_present_6
        if_effectivedate_day_present_6 >> rail.Label('Yes') >> log_gettheaccrualbalancesetup_7 >> parse_json_8 >> \
            log_gettheaccrualbalance_9 >> log_existing_accrual_10 >> log_getthestartingbalancesetup_11 >> log_getthestartingbalancescript_12 >> parse_json_13 >> \
            log_getthestartingbalance_14 >> log_existing_starting_balance_15 >> log_required_numberofdaysforprorationcalculation_16 >> \
            log_required_accrual_17 >> log_required_accrual_json_18 >> if_request_type_equals_to_add_19
        if_request_type_equals_to_add_19 >> rail.Label(
            'Yes') >> log_required_starting_balance_20 >> log_required_starting_balance_json_21 >> log_timeoff_policy_22 >> \
            put_user_time_off_account_policy_set_schedule_23 >> if_request_type_equals_to_update_24
        if_request_type_equals_to_add_19 >> rail.Label(
            'No') >> if_request_type_equals_to_update_24
        if_request_type_equals_to_update_24 >> rail.Label(
            'Yes') >> log_required_accrual_25 >> log_required_accrual_26 >> get_user_time_off_type_policy_summary_28 >> \
            log_existing_policy_summary_43 >> log_effective_policyto_consider_44 >> \
            parse_json_45 >> log_gettheexistingstartingbalancesetup_46 >> log_gettheexistingaccrualsetup_47 >> parse_json_48 >> \
            log_gettheaccrualbalance_49 >> log_existing_accrual_50 >> log_required_accrual_51 >> log_required_accrual_52 >> \
            log_existing_accrual_month_53 >> log_existing_accrual_month_54 >> log_required_accrual_month_55 >> log_required_accrual_month_56 >> \
            log_existing_accrual_date_57 >> log_existing_accrual_date_58 >> log_required_accrual_date_59 >> \
            log_required_accrual_date_60 >> log_policy_to_consider_61 >> log_policy_set_61 >> parse_json_62 >> \
            add_policy_63 >> get_default_time_off_policy_set_schedule_for_time_off_type_64 >> log_effective_dateto_consider_65 >> \
            log_policytoassign_87 >> put_user_time_off_account_policy_set_schedule_88 >> catch_89_89_89
        if_request_type_equals_to_update_24 >> rail.Label(
            'No') >> catch_89_89_89
        if_effectivedate_day_present_6 >> rail.Label(
            'No') >> catch_89_89_89 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
