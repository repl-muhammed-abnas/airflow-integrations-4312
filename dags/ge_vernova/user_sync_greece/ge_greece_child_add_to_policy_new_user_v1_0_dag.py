
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_greece_child_add_to_policy_new_user_v1_0_{config.instance}',
        description=f'GE_Greece_Child Workflow to add timeoff policy for new user v1.0 {config.instance}',
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
            no_task='get_default_time_off_type_policy_schedule_for_user_18'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_default_time_off_type_policy_schedule_for_user_18',
            end_task='add_timeoff_type_logs_23',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_default_time_off_type_policy_schedule_for_user_18 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_18',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeofftypeuri }}"
                }
            }
        )

        log_timeoff_policy_20 = rail.PythonOperator(
            task_id='log_timeoff_policy_20',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_18'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"')) if rail.result('get_default_time_off_type_policy_schedule_for_user_18') else None
        )

        if_log_timeoff_policy_20_present_21 = rail.IfOperator(
            task_id='if_log_timeoff_policy_20_present_21',
            test='''{{ result('log_timeoff_policy_20') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_22",
            no_task="add_timeoff_type_logs_23",
        )

        put_user_time_off_account_policy_set_schedule_22 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_22',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeofftypeuri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_20')
            }
        )

        add_timeoff_type_logs_23 = rail.WriteLogOperator(
            task_id='add_timeoff_type_logs_23',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": ""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> add_timeoff_type_logs_23
        can_run_batch_task >> rail.Label('No') >> get_default_time_off_type_policy_schedule_for_user_18 >> \
            log_timeoff_policy_20 >> if_log_timeoff_policy_20_present_21
        if_log_timeoff_policy_20_present_21 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_22 >> add_timeoff_type_logs_23
        if_log_timeoff_policy_20_present_21 >> rail.Label(
            'No') >> add_timeoff_type_logs_23 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
