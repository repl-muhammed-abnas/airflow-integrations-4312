
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0_{config.instance}',
        description=f'GE_Denmark_Child Vacation -Parttime v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            end_task='catch_48',
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
            test='''{{ result('get_default_time_off_type_policy_schedule_for_user_4') | is_truthy }}''',
            yes_task="log_gettheaccrualbalancesetup_7",
            no_task="catch_48",
        )

        log_gettheaccrualbalancesetup_7 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_7',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "EY GE Denmark - Time Off Accrual Script", "additionalParameters")
        )

        parse_json_8 = rail.PythonOperator(
            task_id='parse_json_8',
            python_callable=lambda: json.loads(json.dumps(
                rail.result('log_gettheaccrualbalancesetup_7')))
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
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_gettheaccrualbalance_9')}},
                ensure_ascii=False)
        )

        if_request_type_equals_to_add_11 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_11',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="log_required_accrual_12",
            no_task="if_request_type_equals_to_update_16",
        )

        log_required_accrual_12 = rail.PythonOperator(
            task_id='log_required_accrual_12',
            python_callable=lambda dag_run: float(float(
                dag_run.conf['numberofworkingdays']) / 5 * rail.result('log_gettheaccrualbalance_9'))
        )

        log_required_accrual_13 = rail.PythonOperator(
            task_id='log_required_accrual_13',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_12')}},
                ensure_ascii=False)
        )

        log_timeoff_policy_14 = rail.PythonOperator(
            task_id='log_timeoff_policy_14',
            python_callable=lambda: json.loads(json.dumps(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4'), ensure_ascii=False).replace(
                rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_13')).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_15 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_15',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_14')
            }
        )

        if_request_type_equals_to_update_16 = rail.IfOperator(
            task_id='if_request_type_equals_to_update_16',
            test='''{{ dag_run.conf.type == 'Update' }}''',
            yes_task="log_tenure_17",
            no_task="catch_48",
        )

        def get_tenure_date(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            accrual_start_date = datetime.strptime(
                dag_run.conf['actualstartdate'], "%d/%m/%Y")
            return (accrual_start_date.timestamp() - start_date.timestamp()) / 86400 / 365

        log_tenure_17 = rail.PythonOperator(
            task_id='log_tenure_17',
            python_callable=get_tenure_date
        )

        def get_proration(dag_run):
            start_date = datetime.strptime(
                dag_run.conf["startdate"], "%d/%m/%Y")
            begining_year = start_date + \
                relativedelta(months=12) + timedelta(days=-1)
            return (begining_year.timestamp() - start_date.timestamp()) / 86400

        log_required_numberofdaysforprorationcalculation_18 = rail.PythonOperator(
            task_id='log_required_numberofdaysforprorationcalculation_18',
            python_callable=get_proration
        )

        log_required_accrual_19 = rail.PythonOperator(
            task_id='log_required_accrual_19',
            python_callable=lambda dag_run: ((float(dag_run.conf['numberofworkingdays']) / 5) * float(rail.result(
                'log_gettheaccrualbalance_9')) / 365) * float(rail.result('log_required_numberofdaysforprorationcalculation_18'))
        )

        log_required_accrual_20 = rail.PythonOperator(
            task_id='log_required_accrual_20',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_required_accrual_19')}},
                ensure_ascii=False)
        )

        declare_list_21 = rail.SetVariableOperator(
            task_id='declare_list_21',
            append=False,
            name='timeoffpolicy',
            value=[]
        )

        get_user_time_off_type_policy_summary_22 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_22',
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

        def get_existing_dk_policy_summary(dag_run):
            to_policy_list = []
            user_time_off_type_policy_summary = rail.result(
                'get_user_time_off_type_policy_summary_22')
            if user_time_off_type_policy_summary['policiesByTimeOffType']:
                for time_off_type_policy_summary in user_time_off_type_policy_summary['policiesByTimeOffType']:
                    if time_off_type_policy_summary['timeOffType']['displayText'] == "01. DK_Vacation":
                        for policy in time_off_type_policy_summary['policySetSchedule']:
                            eff_date = get_datetime_obj(
                                policy['effectiveDate'])
                            start_date = datetime.strptime(
                                dag_run.conf["startdate"], "%d/%m/%Y")
                            if eff_date < start_date:
                                poloicy_set = json.loads(json.dumps(policy['policySet'], ensure_ascii=False).replace(
                                    rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_20')).replace('null', '"effective"').replace(
                                    '"script"', '"scriptTarget"'))
                                to_policy_list.append({
                                    "description": policy['description'],
                                    "effectiveDate": {
                                        "day": eff_date.day,
                                        "month": eff_date.month,
                                        "year": eff_date.year,
                                    },
                                    "policySet": poloicy_set
                                })
            return to_policy_list

        log_dkvacation_policy_28 = rail.PythonOperator(
            task_id='log_dkvacation_policy_28',
            python_callable=get_existing_dk_policy_summary
        )

        get_default_time_off_policy_set_schedule_for_time_off_type_29 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type_29',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        def get_existing_policy(dag_run):
            count_of_policy_tobe_assigned = []
            existing_policies = rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_29') if rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type_29') else []
            for policy in existing_policies:
                if policy['startOffset']['offsetValue'] >= rail.result('log_tenure_17'):
                    count_of_policy_tobe_assigned.append({
                        "count": int(policy['startOffset']['offsetValue']),
                        "policy": policy['policySet']
                    })
            if len(count_of_policy_tobe_assigned) == 0:
                for policy in existing_policies:
                    count_of_policy_tobe_assigned.append({
                        "count": int(policy['startOffset']['offsetValue']),
                        "policy": policy['policySet']
                    })

            index_no = 0
            to_policy_list = rail.result('log_dkvacation_policy_28') if rail.result(
                'log_dkvacation_policy_28') else []
            for count_policy in count_of_policy_tobe_assigned:
                poloicy_set = json.loads(json.dumps(count_policy['policy'], ensure_ascii=False).replace(
                    rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_20')).replace('null', '"effective"').replace(
                    '"script"', '"scriptTarget"'))
                if index_no == 0:
                    start_date = datetime.strptime(
                        dag_run.conf["startdate"], "%d/%m/%Y")
                else:
                    month_tobe_add = count_policy['count'] * 12
                    start_date = datetime.strptime(
                        dag_run.conf["startdate"], "%d/%m/%Y") + relativedelta(months=month_tobe_add)
                to_policy_list.append({
                    "description": "Effective on" + dag_run.conf["startdate"],
                    "effectiveDate": {
                        "day": start_date.day,
                        "month": start_date.month,
                        "year": start_date.year,
                    },
                    "policySet": poloicy_set
                })

            return to_policy_list

        log_policy_set_39 = rail.PythonOperator(
            task_id='log_policy_set_39',
            python_callable=lambda: json.loads(json.dumps(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4'), ensure_ascii=False).replace(
                rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_20')).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_48 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_48',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": get_existing_policy(dag_run)
            }
        )

        catch_48 = rail.EmptyOperator(
            task_id='catch_48',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_48
        can_run_batch_task >> rail.Label(
            'No') >> get_default_time_off_type_policy_schedule_for_user_4 >> if_effectivedate_day_present_6
        if_effectivedate_day_present_6 >> rail.Label(
            'Yes') >> log_gettheaccrualbalancesetup_7 >> parse_json_8 >> log_gettheaccrualbalance_9 >> \
            log_existing_accrual_10 >> if_request_type_equals_to_add_11
        if_request_type_equals_to_add_11 >> rail.Label(
            'Yes') >> log_required_accrual_12 >> log_required_accrual_13 >> log_timeoff_policy_14 >> \
            put_user_time_off_account_policy_set_schedule_15 >> if_request_type_equals_to_update_16
        if_request_type_equals_to_add_11 >> rail.Label(
            'No') >> if_request_type_equals_to_update_16
        if_request_type_equals_to_update_16 >> rail.Label(
            'Yes') >> log_tenure_17 >> log_required_numberofdaysforprorationcalculation_18 >> \
            log_required_accrual_19 >> log_required_accrual_20 >> declare_list_21 >> get_user_time_off_type_policy_summary_22 >> \
            log_dkvacation_policy_28 >> get_default_time_off_policy_set_schedule_for_time_off_type_29 >> log_policy_set_39 >> put_user_time_off_account_policy_set_schedule_48 >> catch_48
        if_request_type_equals_to_update_16 >> rail.Label(
            'No') >> catch_48
        if_effectivedate_day_present_6 >> rail.Label(
            'No') >> catch_48 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
