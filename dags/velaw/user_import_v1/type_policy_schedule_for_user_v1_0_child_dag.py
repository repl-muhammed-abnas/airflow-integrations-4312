
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.type_policy_schedule_for_user_child_dag_id,
        description=f'VelawG3 Child Time off Type Policy Schedule V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
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
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_default_time_off_type_policy_schedule_for_user_18 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_18',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeofftypeuri']
                }
            }
        )

        log_policyto_assign_19 = rail.PythonOperator(
            task_id='log_policyto_assign_19',
            python_callable=lambda: json.loads(json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_18'), ensure_ascii=False).replace("null", '"effective"')
                                               .replace('"script"', '"scriptTarget"')) if rail.result('get_default_time_off_type_policy_schedule_for_user_18') else null
        )

        if_log_policyto_assign_19_present_20 = rail.IfOperator(
            task_id='if_log_policyto_assign_19_present_20',
            test='''{{ result('log_policyto_assign_19') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_21",
            no_task="log_to_sumo",
        )

        put_user_time_off_account_policy_set_schedule_21 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_21',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeofftypeuri']
                },
                "policySetScheduleEntries": rail.result('log_policyto_assign_19')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_default_time_off_type_policy_schedule_for_user_18 \
            >> log_policyto_assign_19 >> if_log_policyto_assign_19_present_20
        if_log_policyto_assign_19_present_20 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_21 >> log_to_sumo
        if_log_policyto_assign_19_present_20 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
