from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from momentive.user_import_india.utils.python_callable import split_date_string

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_india_user_sync_child_disable_user_dag_id,
        description=f'Momentive_user_sync_disable_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
            no_task='if_request_user_id_equals_to_repliconadmin_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_user_id_equals_to_repliconadmin_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_user_id_equals_to_repliconadmin_3 = rail.IfOperator(
            task_id='if_request_user_id_equals_to_repliconadmin_3',
            test='''{{ dag_run.conf.userid == 'replicon.admin' }}''',
            yes_task="catch_and_log_error",
            no_task="get_split_start_and_end_dates",
        )

        get_split_start_and_end_dates = rail.PythonOperator(
            task_id="get_split_start_and_end_dates",
            python_callable=lambda dag_run: {
                "startdate_split": split_date_string(dag_run.conf['hiredate']),
                "enddate_split": split_date_string(dag_run.conf['terminationdate'])
            }
        )

        if_termination_date_to_date_lesser_or_equals_to_today_7 = rail.IfOperator(
            task_id='if_termination_date_to_date_lesser_or_equals_to_today_7',
            test=lambda dag_run: bool(datetime.strptime(
                dag_run.conf['terminationdate'], "%Y-%m-%d") <= datetime.now()),
            yes_task="disable_login_8",
            no_task="update_employment_date_rangeforenddate_9",
        )

        disable_login_8 = rail.RepliconServiceOperator(
            task_id='disable_login_8',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        update_employment_date_rangeforenddate_9 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate_9',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('get_split_start_and_end_dates').startdate_split.year }}",
                        "month": "{{ result('get_split_start_and_end_dates').startdate_split.month }}",
                        "day": "{{ result('get_split_start_and_end_dates').startdate_split.day }}"
                    },
                    "endDate": {
                        "year": "{{ result('get_split_start_and_end_dates').enddate_split.year }}",
                        "month": "{{ result('get_split_start_and_end_dates').enddate_split.month }}",
                        "day": "{{ result('get_split_start_and_end_dates').enddate_split.day }}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_user_time_off_type_policy_summary_10 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_10',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_all_scripts_time_offbalanceeventscripts_11 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_offbalanceeventscripts_11',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        get_all_scripts_time_offvalidationscripts_12 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_offvalidationscripts_12',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
        )

        foreach_d_13 = rail.ForEachOperator(
            task_id='foreach_d_13',
            items=lambda: rail.result('get_user_time_off_type_policy_summary_10')[
                'policiesByTimeOffType'],
            start_task='if_13_istimeoffallowedagainstthistimeofftype_is_true_14',
            end_task='foreach_d_13_end'
        )

        if_13_istimeoffallowedagainstthistimeofftype_is_true_14 = rail.IfOperator(
            task_id='if_13_istimeoffallowedagainstthistimeofftype_is_true_14',
            test='''{{ result('foreach_d_13').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="get_balance_summary_for_account_15",
            no_task="foreach_d_13_end",
        )

        get_balance_summary_for_account_15 = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account_15',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data={
                "account": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_13').timeOffType.uri }}"
                },
                "asOfDate": {
                    "year": "{{ result('get_split_start_and_end_dates').enddate_split.year }}",
                    "month": "{{ result('get_split_start_and_end_dates').enddate_split.month }}",
                    "day": "{{ result('get_split_start_and_end_dates').enddate_split.day }}"
                }
            }
        )

        if_first_description_present_16 = rail.IfOperator(
            task_id='if_first_description_present_16',
            test='''{{ result('foreach_d_13').policySetSchedule[0].description | is_truthy }}''',
            yes_task="trigger_dag_run_momentive_put_remaining_balance_for_payout_18",
            no_task="foreach_d_13_end",
        )

        trigger_dag_run_momentive_put_remaining_balance_for_payout_18 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_momentive_put_remaining_balance_for_payout_18',
            retries=0,
            trigger_dag_id=config.momentive_india_user_sync_child_put_remaining_balance_for_payout_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "timeoffuri": rail.result('foreach_d_13')['timeOffType']['uri'],
                "useruri": dag_run.conf['useruri'],
                "terminationdate": rail.result('get_split_start_and_end_dates')['enddate_split']['day'] + "/" + rail.result('get_split_start_and_end_dates')['enddate_split']['month'] + "/" + rail.result('get_split_start_and_end_dates')['enddate_split']['year'],
                "startingbalancesettouri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_offbalanceeventscripts_11'), 'displayText', "Starting Balance Set To", 'uri', ''),
                "balance": int(rail.result('get_balance_summary_for_account_15')['timeRemaining']) if rail.result('get_balance_summary_for_account_15')['timeRemaining'] else 0
            }
        )

        wait_for_completion_trigger_dag_run_momentive_put_remaining_balance_for_payout_18 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_momentive_put_remaining_balance_for_payout_18',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_momentive_put_remaining_balance_for_payout_18") }}'
        )

        foreach_d_13_end = rail.EmptyOperator(
            task_id='foreach_d_13_end',
        )

        momentive_user_import_logs_add_entry_21 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_21',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.userid }}",
                "username": "{{ dag_run.conf.firstname }}" + "|" + "{{ dag_run.conf.lastname }}",
                "action": "Disable user",
                "status": "Success",
                "details": "User profile disabled successfully",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Disable user",
                "status": "Error",
                "details": "Error processing Disabling user - " + rail.render_template("{{get_error_message()}}"),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> if_request_user_id_equals_to_repliconadmin_3

        if_request_user_id_equals_to_repliconadmin_3 >> rail.Label(
            'Yes') >> catch_and_log_error

        if_request_user_id_equals_to_repliconadmin_3 >> rail.Label(
            'No') >> get_split_start_and_end_dates >> if_termination_date_to_date_lesser_or_equals_to_today_7

        if_termination_date_to_date_lesser_or_equals_to_today_7 >> rail.Label(
            'No') >> update_employment_date_rangeforenddate_9
        if_termination_date_to_date_lesser_or_equals_to_today_7 >> rail.Label(
            'Yes') >> disable_login_8 >> update_employment_date_rangeforenddate_9

        update_employment_date_rangeforenddate_9 >> get_user_time_off_type_policy_summary_10 >> get_all_scripts_time_offbalanceeventscripts_11 \
            >> get_all_scripts_time_offvalidationscripts_12 >> foreach_d_13 >> if_13_istimeoffallowedagainstthistimeofftype_is_true_14

        if_13_istimeoffallowedagainstthistimeofftype_is_true_14 >> rail.Label(
            'No') >> foreach_d_13_end
        if_13_istimeoffallowedagainstthistimeofftype_is_true_14 >> rail.Label(
            'Yes') >> get_balance_summary_for_account_15 >> if_first_description_present_16

        if_first_description_present_16 >> rail.Label('No') >> foreach_d_13_end
        if_first_description_present_16 >> rail.Label('Yes') >> trigger_dag_run_momentive_put_remaining_balance_for_payout_18 \
            >> wait_for_completion_trigger_dag_run_momentive_put_remaining_balance_for_payout_18 >> foreach_d_13_end

        foreach_d_13 >> foreach_d_13_end >> momentive_user_import_logs_add_entry_21 >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
