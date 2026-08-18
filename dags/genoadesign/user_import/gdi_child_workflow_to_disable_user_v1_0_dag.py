
from datetime import timedelta, datetime
import pendulum
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'genoadesign_user_import_gdi_child_workflow_to_disable_user_v1_0_{config.instance}',
        description=f'Live|GDI_Child_Workflow to disable user V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
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
            no_task='disable_login_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='disable_login_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        disable_login_3 = rail.RepliconServiceOperator(
            task_id='disable_login_3',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        update_employment_date_rangeforenddate_10 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate_10',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['startdate'], '%Y%m%d').year,
                        "month": datetime.strptime(dag_run.conf['startdate'], '%Y%m%d').month,
                        "day": datetime.strptime(dag_run.conf['startdate'], '%Y%m%d').day
                    },
                    "endDate": {
                        "year": pendulum.now(config.pacific_timezone).year,
                        "month": pendulum.now(config.pacific_timezone).month,
                        "day": pendulum.now(config.pacific_timezone).day
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_user_time_off_type_policy_summary_11 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_11',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        declare_list_dag_runs_12 = rail.SetVariableOperator(
            task_id='declare_list_dag_runs_12',
            name='topolicy_process_dag_runs',
            value=[]
        )

        foreach_d_12 = rail.ForEachOperator(
            task_id='foreach_d_12',
            items="{{ result('get_user_time_off_type_policy_summary_11').policiesByTimeOffType | to_json}}",
            start_task='if_foreach_d_12_istimeoffallowedagainstthistimeofftype_is_true_13',
            end_task='foreach_d_12_end'
        )

        if_foreach_d_12_istimeoffallowedagainstthistimeofftype_is_true_13 = rail.IfOperator(
            task_id='if_foreach_d_12_istimeoffallowedagainstthistimeofftype_is_true_13',
            test='''{{ result('foreach_d_12').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="get_balance_summary_for_account_14",
            no_task="foreach_d_12_end",
        )

        get_balance_summary_for_account_14 = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account_14',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_d_12')['timeOffType']['uri']
                },
                "asOfDate": {
                    "year": pendulum.now(config.pacific_timezone).year,
                    "month": pendulum.now(config.pacific_timezone).month,
                    "day": pendulum.now(config.pacific_timezone).day
                }
            }
        )

        log_policy_set_schedule_15 = rail.PythonOperator(
            task_id='log_policy_set_schedule_15',
            python_callable=lambda:  rail.result(
                'foreach_d_12')['policySetSchedule']
        )

        if_log_policy_set_schedule_15_present_16 = rail.IfOperator(
            task_id='if_log_policy_set_schedule_15_present_16',
            test='''{{ result('log_policy_set_schedule_15') | is_truthy }}''',
            yes_task="trigger_dag_run_genoadesign_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_018",
            no_task="foreach_d_12_end",
        )

        trigger_dag_run_genoadesign_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_018 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_genoadesign_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_018',
            retries=0,
            items=[-1],
            trigger_dag_id=f'genoadesign_user_import_gdi_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "enddate": pendulum.now(config.pacific_timezone).strftime('%Y%m%d'),
                "timeoffuri": rail.result('foreach_d_12')['timeOffType']['uri'],
                "policyset": rail.result('log_policy_set_schedule_15'),
                "newschedulebalance": rail.result('get_balance_summary_for_account_14')['timeRemaining']
            }
        )

        insert_to_timeoff_dag_run_list_18 = rail.SetVariableOperator(
            task_id='insert_to_timeoff_dag_run_list_18',
            append=True,
            name='{{ result("declare_list_dag_runs_12").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_genoadesign_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_018"))[0]}}'
        )

        foreach_d_12_end = rail.EmptyOperator(
            task_id='foreach_d_12_end',
        )

        is_topolicy_trigger_avaialbale_18 = rail.IfOperator(
            task_id='is_topolicy_trigger_avaialbale_18',
            test='''{{ result('insert_to_timeoff_dag_run_list_18') | is_truthy }}''',
            yes_task="wait_for_completion_trigger_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_018",
            no_task="genoadi_user_import_logs_add_entry_23",
        )

        wait_for_completion_trigger_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_018 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_018',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_timeoff_dag_run_list_18").value | to_json }}'
        )

        genoadi_user_import_logs_add_entry_21 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_21',
            message="na",
            severity="Success",
            properties={
                "username|loginname": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} |{{ dag_run.conf.userloginname }} ",
                "status": "Success",
                "childjobid": "{{ dag_run.conf.childjobid }}-{{ dag_run_ecid() }}",
                "details":  "User profile disabled successfully"
            }
        )

        genoadi_user_import_logs_add_entry_23 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_23',
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "username|loginname": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} |{{ dag_run.conf.userloginname }} ",
                "status": "Error",
                "childjobid": "{{ dag_run.conf.childjobid }}-{{ dag_run_ecid() }}",
                "details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> disable_login_3
        disable_login_3 >> update_employment_date_rangeforenddate_10 >> get_user_time_off_type_policy_summary_11 >> \
            declare_list_dag_runs_12 >> foreach_d_12 >> if_foreach_d_12_istimeoffallowedagainstthistimeofftype_is_true_13
        if_foreach_d_12_istimeoffallowedagainstthistimeofftype_is_true_13 >> rail.Label(
            'Yes') >> get_balance_summary_for_account_14 >> log_policy_set_schedule_15 >> if_log_policy_set_schedule_15_present_16
        if_log_policy_set_schedule_15_present_16 >> rail.Label(
            'Yes') >> trigger_dag_run_genoadesign_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_018 >> \
            insert_to_timeoff_dag_run_list_18 >> foreach_d_12_end
        if_log_policy_set_schedule_15_present_16 >> rail.Label(
            'No') >> foreach_d_12_end
        if_foreach_d_12_istimeoffallowedagainstthistimeofftype_is_true_13 >> rail.Label(
            'No') >> foreach_d_12_end
        foreach_d_12 >> foreach_d_12_end >> is_topolicy_trigger_avaialbale_18
        is_topolicy_trigger_avaialbale_18 >> rail.Label(
            'Yes') >> genoadi_user_import_logs_add_entry_23
        is_topolicy_trigger_avaialbale_18 >> rail.Label('Yes') >> \
            wait_for_completion_trigger_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v1_018 >> \
            genoadi_user_import_logs_add_entry_21 >> \
            genoadi_user_import_logs_add_entry_23 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
