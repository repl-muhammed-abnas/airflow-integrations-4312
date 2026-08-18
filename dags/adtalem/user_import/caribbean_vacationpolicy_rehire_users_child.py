from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.python_callable_method import construct_policyschedule
from adtalem.user_import.utils.request_payload import get_user_tenure
from adtalem.user_import.utils.response_filter import get_final_policysets


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_vacationpolicy_rehire_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_caribbean_child_assign_policy_for_rehire_update_users_{config.instance}',
        description=f'Adtalem Carribean assign policy for Rehire/Update users_Prod {config.instance}',
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
            no_task='get_existingpolicy_schedule_for_timeoff'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_existingpolicy_schedule_for_timeoff',
            end_task='dagrun_log_to_sumo',
        )

        get_existingpolicy_schedule_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_existingpolicy_schedule_for_timeoff',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        is_existingpolicy_schedule = rail.IfOperator(
            task_id='is_existingpolicy_schedule',
            test="{{ result('get_existingpolicy_schedule_for_timeoff') | is_truthy }}",
            yes_task="past_policyset_schedule",
            no_task="get_usertenure_servicedate",
        )

        past_policyset_schedule = rail.PythonOperator(
            task_id='past_policyset_schedule',
            python_callable=construct_policyschedule
        )

        get_usertenure_servicedate = rail.PythonOperator(
            task_id='get_usertenure_servicedate',
            python_callable=get_user_tenure,
            op_args=['{{ dag_run.conf.servicedate }}']
        )

        get_final_policysets_schedule = rail.RepliconServiceOperator(
            task_id='get_final_policysets_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            },
            data_handler=get_final_policysets
        )

        put_user_timeoff_account_policysetschedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policysetschedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_final_policysets_schedule')
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
            'No') >> get_existingpolicy_schedule_for_timeoff

        get_existingpolicy_schedule_for_timeoff >> is_existingpolicy_schedule
        is_existingpolicy_schedule >> rail.Label(
            'Yes') >> past_policyset_schedule >> get_usertenure_servicedate
        is_existingpolicy_schedule >> rail.Label(
            'No') >> get_usertenure_servicedate

        get_usertenure_servicedate >> get_final_policysets_schedule >> \
            put_user_timeoff_account_policysetschedule >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_vacationpolicy_rehire_child_dag)
