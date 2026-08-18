
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_netherlands_child_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_0_{config.instance}',
        description=f'GE Netherlands_Child Timeoff type Proration Assignment for 06. NL_Wedding Leave v1.0 {config.instance}',
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
            end_task='catch_21_21_21',
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
            test='''{{ result('get_default_time_off_type_policy_schedule_for_user_4')[0].effectiveDate.day | is_truthy }}''',
            yes_task="log_gettheaccrualbalancesetup_7",
            no_task="catch_21_21_21",
        )

        log_gettheaccrualbalancesetup_7 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_7',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "Yearly Accrual", "additionalParameters")
        )

        log_gettheaccrualbalance_9 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_9',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'], 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')
        )

        log_existing_accrual_10 = rail.PythonOperator(
            task_id='log_existing_accrual_10',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": rail.result('log_gettheaccrualbalance_9')}},
                ensure_ascii=False)
        )

        log_getthestartingbalancesetup_11 = rail.PythonOperator(
            task_id='log_getthestartingbalancesetup_11',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "Starting Balance Set To", "additionalParameters")
        )

        parse_json_13 = rail.PythonOperator(
            task_id='parse_json_13',
            python_callable=lambda: rail.result(
                'log_getthestartingbalancesetup_11')
        )

        log_getthestartingbalance_14 = rail.PythonOperator(
            task_id='log_getthestartingbalance_14',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_13'), 'keyUri', 'urn:replicon:script-key:parameter:amount', 'value.number')
        )

        log_existing_starting_balance_15 = rail.PythonOperator(
            task_id='log_existing_starting_balance_15',
            python_callable=lambda:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": rail.result('log_getthestartingbalance_14')}},
                ensure_ascii=False)
        )

        log_required_accrual_json_16 = rail.PythonOperator(
            task_id='log_required_accrual_json_16',
            python_callable=lambda dag_run:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": dag_run.conf['accrual']}},
                ensure_ascii=False)
        )

        if_request_type_equals_to_add_17 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_17',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="log_required_starting_balance_json_18",
            no_task="catch_21_21_21",
        )

        log_required_starting_balance_json_18 = rail.PythonOperator(
            task_id='log_required_starting_balance_json_18',
            python_callable=lambda dag_run:  json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": dag_run.conf['accrual']}},
                ensure_ascii=False)
        )

        log_timeoff_policy_19 = rail.PythonOperator(
            task_id='log_timeoff_policy_19',
            python_callable=lambda:  json.loads(json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_4'), ensure_ascii=False).replace(rail.result('log_existing_starting_balance_15'), rail.result(
                'log_required_starting_balance_json_18')).replace(rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_json_16')).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_20 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_20',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_19')
            }
        )

        catch_21_21_21 = rail.EmptyOperator(
            task_id='catch_21_21_21',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_21_21_21
        can_run_batch_task >> rail.Label(
            'No') >> get_default_time_off_type_policy_schedule_for_user_4 >> if_effectivedate_day_present_6
        if_effectivedate_day_present_6 >> rail.Label('Yes') >> log_gettheaccrualbalancesetup_7 >> \
            log_gettheaccrualbalance_9 >> log_existing_accrual_10 >> log_getthestartingbalancesetup_11 >> \
            parse_json_13 >> log_getthestartingbalance_14 >> log_existing_starting_balance_15 >> \
            log_required_accrual_json_16 >> if_request_type_equals_to_add_17
        if_request_type_equals_to_add_17 >> rail.Label('Yes') >> log_required_starting_balance_json_18 >> \
            log_timeoff_policy_19 >> put_user_time_off_account_policy_set_schedule_20 >> catch_21_21_21
        if_request_type_equals_to_add_17 >> rail.Label('No') >> catch_21_21_21
        if_effectivedate_day_present_6 >> rail.Label(
            'No') >> catch_21_21_21 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
