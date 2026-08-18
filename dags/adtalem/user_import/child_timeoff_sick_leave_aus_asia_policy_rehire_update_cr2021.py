
from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.request_payload import get_user_tenure
from adtalem.user_import.utils.response_filter import get_final_policysets2, get_usertimeoff_policy_by_effectivedate


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_timeoff_sickleave_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_user_import_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_{config.instance}',
        description=f'Timeoff_Sick leave Aus_Asia Policy Rehire/Update_CR2021_V1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_type_rehire'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_type_rehire',
            end_task='dagrun_log_to_sumo',
        )

        is_type_rehire = rail.IfOperator(
            task_id='is_type_rehire',
            test="{{ dag_run.conf.type == 'Rehire' }}",
            yes_task="get_usertenure_servicedate",
            no_task="dagrun_log_to_sumo",
        )

        get_usertenure_servicedate = rail.PythonOperator(
            task_id='get_usertenure_servicedate',
            python_callable=get_user_tenure,
            op_args=['{{ dag_run.conf.servicedate }}',
                     "{{ dag_run.conf.rehiredate if 'Rehire' in dag_run.conf.type else '' }}"]
        )

        get_user_timeoff_policy = rail.RepliconServiceOperator(
            task_id='get_user_timeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_usertimeoff_policy_by_effectivedate
        )

        get_default_timeoff_policy_set_schedule_for_time_off_type = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_policy_set_schedule_for_time_off_type',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            },
            data_handler=get_final_policysets2
        )

        is_final_policy_sets = rail.IfOperator(
            task_id='is_final_policy_sets',
            test="{{ result('get_default_timeoff_policy_set_schedule_for_time_off_type') | is_truthy }}",
            yes_task="put_user_time_off_account_policy_set_schedule",
            no_task="dagrun_log_to_sumo",
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_default_timeoff_policy_set_schedule_for_time_off_type')
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> is_type_rehire

        is_type_rehire >> rail.Label(
            'Yes') >> get_usertenure_servicedate >> get_user_timeoff_policy >> \
            get_default_timeoff_policy_set_schedule_for_time_off_type >> \
            is_final_policy_sets

        is_final_policy_sets >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule
        is_final_policy_sets >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_type_rehire >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_timeoff_sickleave_dag)
