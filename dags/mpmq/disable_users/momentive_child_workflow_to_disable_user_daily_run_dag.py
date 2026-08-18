
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'mpmq_disable_users_momentive_child_workflow_to_disable_user_daily_run_{config.instance}',
        description=f'Momentive_Child_Workflow to disable user Daily Run {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_tenant_and_useridentity_details_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_tenant_and_useridentity_details_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_tenant_and_useridentity_details_3 = rail.RepliconServiceOperator(
            task_id='get_tenant_and_useridentity_details_3',
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity",
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        if_request_loginname_equals_to_integration_user_5 = rail.IfOperator(
            task_id='if_request_loginname_equals_to_integration_user_5',
            test='''{{ dag_run.conf.loginname == result('get_tenant_and_useridentity_details_3').loginName }}''',
            yes_task="stop_6",
            no_task="invoke_custom_ruby_code_7",
        )

        stop_6 = rail.EmptyOperator(
            task_id='stop_6',

        )

        invoke_custom_ruby_code_7 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_7',
            python_callable=lambda: rail.parse_date(
                rail.get_dag_run_conf()['terminationdate'], '%Y/%m/%d')
        )

        disable_login_8 = rail.RepliconServiceOperator(
            task_id='disable_login_8',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_user_time_off_type_policy_summary_9 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_9',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_all_scripts_time_offbalanceeventscripts_10 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_offbalanceeventscripts_10',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        get_all_scripts_time_offvalidationscripts_11 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_offvalidationscripts_11',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
        )

        foreach_timeoff_policies_12 = rail.ForEachOperator(
            task_id='foreach_timeoff_policies_12',
            items="{{ result('get_user_time_off_type_policy_summary_9').policiesByTimeOffType | to_json }}",
            start_task='if_foreach_timeoff_policies_12_istimeoffallowedagainstthistimeofftype_is_true_13',
            end_task='foreach_timeoff_policies_12_end'
        )

        if_foreach_timeoff_policies_12_istimeoffallowedagainstthistimeofftype_is_true_13 = rail.IfOperator(
            task_id='if_foreach_timeoff_policies_12_istimeoffallowedagainstthistimeofftype_is_true_13',
            test='''{{ result('foreach_timeoff_policies_12').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="get_balance_summary_for_account_14",
            no_task="foreach_timeoff_policies_12_end",
        )

        get_balance_summary_for_account_14 = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account_14',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data={
                "account": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_timeoff_policies_12').timeOffType.uri }}"
                },
                "asOfDate": {
                    "year": "{{result('invoke_custom_ruby_code_7').year}}",
                    "month": "{{result('invoke_custom_ruby_code_7').month}}",
                    "day": "{{result('invoke_custom_ruby_code_7').day}}"
                }
            }
        )

        if_first_description_present_15 = rail.IfOperator(
            task_id='if_first_description_present_15',
            test='''{{ result('foreach_timeoff_policies_12').policySetSchedule | is_truthy }}''',
            yes_task="trigger_dag_run_mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout17",
            no_task="foreach_timeoff_policies_12_end",
        )

        trigger_dag_run_mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout17 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout17',
            retries=0,
            items=[1],
            trigger_dag_id=f'mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "timeoffuri": rail.render_template("{{ result('foreach_timeoff_policies_12').timeOffType.uri }}"),
                "useruri": rail.render_template("{{ dag_run.conf.useruri }}"),
                "terminationdate": rail.result('invoke_custom_ruby_code_7'),
                "startingbalancesettouri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_offbalanceeventscripts_10'), 'displayText', "Starting Balance Set To", ('uri')),
                "balance": float(rail.result('get_balance_summary_for_account_14')['timeRemaining']) if rail.result('get_balance_summary_for_account_14')['timeRemaining'] else 0
            }
        )

        foreach_timeoff_policies_12_end = rail.EmptyOperator(
            task_id='foreach_timeoff_policies_12_end',
        )

        wait_for_completion_trigger_dag_run_mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout17 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout17',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout17") or [] }}'
        )

        momentive_disable_user_logs_add_entry_20 = rail.WriteLogOperator(
            task_id='momentive_disable_user_logs_add_entry_20',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            properties={
                "user_name": "{{ dag_run.conf.username }}",
                "login_name": "{{ dag_run.conf.loginname }}",
                "useruri": "{{dag_run.conf.useruri }}",
                "status": "Success",
                "details": "User profile disabled successfully"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties={
                "user_name": "{{ dag_run.conf.username }}",
                "login_name": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_tenant_and_useridentity_details_3
        get_tenant_and_useridentity_details_3 >> create_log >> if_request_loginname_equals_to_integration_user_5
        if_request_loginname_equals_to_integration_user_5 >> rail.Label(
            'Yes') >> stop_6 >> finish
        if_request_loginname_equals_to_integration_user_5 >> rail.Label(
            'No') >> invoke_custom_ruby_code_7 >> disable_login_8 >> get_user_time_off_type_policy_summary_9 >> get_all_scripts_time_offbalanceeventscripts_10 >> get_all_scripts_time_offvalidationscripts_11 >> foreach_timeoff_policies_12 >> if_foreach_timeoff_policies_12_istimeoffallowedagainstthistimeofftype_is_true_13
        if_foreach_timeoff_policies_12_istimeoffallowedagainstthistimeofftype_is_true_13 >> rail.Label(
            'Yes') >> get_balance_summary_for_account_14 >> if_first_description_present_15
        if_first_description_present_15 >> rail.Label(
            'Yes') >> trigger_dag_run_mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout17 >> foreach_timeoff_policies_12_end
        if_first_description_present_15 >> rail.Label(
            'No') >> foreach_timeoff_policies_12_end
        if_foreach_timeoff_policies_12_istimeoffallowedagainstthistimeofftype_is_true_13 >> rail.Label(
            'No') >> foreach_timeoff_policies_12_end
        foreach_timeoff_policies_12 >> foreach_timeoff_policies_12_end >> wait_for_completion_trigger_dag_run_mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout17 >> momentive_disable_user_logs_add_entry_20 >> finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
