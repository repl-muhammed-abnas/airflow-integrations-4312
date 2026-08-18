from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.request_payload import get_user_tenure
# pylint: disable=line-too-long
from adtalem.user_import.utils.response_filter import get_final_policysets_anniversary_add, get_final_policysets_anniversary_update_rehire, get_usertimeoff_policy_by_effectivedate_plus_tenure


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_anniversaryday_policy_update(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_timeoff_anniversaryday_policyaddupdate_cr14.0_{config.instance}',
        description=f'Timeoff_Anniversary Day Policy Add/Update_CR14.0 {config.instance}',
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
            no_task='is_type_update_rehire'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_type_update_rehire',
            end_task='dagrun_log_to_sumo'
        )

        is_type_update_rehire = rail.IfOperator(
            task_id='is_type_update_rehire',
            test=lambda dag_run: dag_run.conf['type'] in ('Update', 'Rehire'),
            yes_task="get_usertenure_servicedate",
            no_task="is_type_update_add",
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
            data_handler=get_usertimeoff_policy_by_effectivedate_plus_tenure
        )

        get_default_timeoffpolicy_setschedule_for_timeoff_type_rehire = rail.RepliconServiceOperator(
            task_id='get_default_timeoffpolicy_setschedule_for_timeoff_type_rehire',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            },
            data_handler=get_final_policysets_anniversary_update_rehire
        )

        is_final_policy_sets_update = rail.IfOperator(
            task_id='is_final_policy_sets_update',
            test="{{ result('get_default_timeoffpolicy_setschedule_for_timeoff_type_rehire') | is_truthy }}",
            yes_task="put_user_time_off_account_policy_set_schedule_rehire_update",
            no_task="dagrun_log_to_sumo",
        )

        put_user_time_off_account_policy_set_schedule_rehire_update = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_rehire_update',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_default_timeoffpolicy_setschedule_for_timeoff_type_rehire')
            }
        )

        is_type_update_add = rail.IfOperator(
            task_id='is_type_update_add',
            test="{{ dag_run.conf.type == 'Add' }}",
            yes_task="get_default_timeoffpolicy_setschedule_for_timeoff_type_add",
            no_task="dagrun_log_to_sumo",
        )

        get_default_timeoffpolicy_setschedule_for_timeoff_type_add = rail.RepliconServiceOperator(
            task_id='get_default_timeoffpolicy_setschedule_for_timeoff_type_add',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            },
            data_handler=get_final_policysets_anniversary_add
        )

        is_final_policy_sets_add = rail.IfOperator(
            task_id='is_final_policy_sets_add',
            test="{{ result('get_default_timeoffpolicy_setschedule_for_timeoff_type_add') | is_truthy }}",
            yes_task="put_user_time_off_account_policy_set_schedule_add",
            no_task="dagrun_log_to_sumo",
        )

        put_user_time_off_account_policy_set_schedule_add = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_add',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_default_timeoffpolicy_setschedule_for_timeoff_type_add')
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
            'No') >> is_type_update_rehire
        is_type_update_rehire >> rail.Label(
            'Yes') >> get_usertenure_servicedate >> get_user_timeoff_policy >> \
            get_default_timeoffpolicy_setschedule_for_timeoff_type_rehire >> is_final_policy_sets_update
        is_final_policy_sets_update >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_rehire_update >> \
            dagrun_log_to_sumo
        is_final_policy_sets_update >> rail.Label(
            'No') >> dagrun_log_to_sumo
        is_type_update_rehire >> rail.Label(
            'No') >> is_type_update_add
        is_type_update_add >> rail.Label(
            'Yes') >> get_default_timeoffpolicy_setschedule_for_timeoff_type_add >> is_final_policy_sets_add
        is_final_policy_sets_add >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_add >> dagrun_log_to_sumo
        is_final_policy_sets_add >> rail.Label(
            'No') >> dagrun_log_to_sumo
        is_type_update_add >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_anniversaryday_policy_update)
