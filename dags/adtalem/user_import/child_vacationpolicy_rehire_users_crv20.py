from datetime import timedelta
import rail
from airflow.models import Variable
from adtalem.user_import.utils.python_callable_method import get_vacation_timeoff_policyschedule
from adtalem.user_import.utils.request_payload import get_put_vacation_policy, get_put_vacation_policy_enabled

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_vacationpolicy_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_assign_vacation_policy_rehire_users_crv2.0_{config.instance}',
        description=f'Adtalem assign vacation policy for Rehire users_production CRV2.0 {config.instance}',
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
            no_task='get_required_vacation_timeoff_policyschedule'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_required_vacation_timeoff_policyschedule',
            end_task='dagrun_log_to_sumo',
        )

        get_required_vacation_timeoff_policyschedule = rail.PythonOperator(
            task_id='get_required_vacation_timeoff_policyschedule',
            python_callable=get_vacation_timeoff_policyschedule,
            op_args=[config.adtalem_vacation_timeoffpolicy_schedule_mapper_old,
                     '{{ dag_run.conf.policyname }}']
        )

        is_userstatus_disabled = rail.IfOperator(
            task_id='is_userstatus_disabled',
            test="{{ dag_run.conf.status == 'Disabled' }}",
            yes_task="put_vacation_policy_rehire",
            no_task="get_required_vacation_timeoff_policyschedule_enabled",
        )

        put_vacation_policy_rehire = rail.RepliconServiceOperator(
            task_id='put_vacation_policy_rehire',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: get_put_vacation_policy(dag_run.conf['useruri'],
                                                         config.first_timeofftype_in_instance,
                                                         dag_run.conf['rehire'],
                                                         dag_run.conf['policyname'],
                                                         dag_run.conf['previouspolicy'])
        )

        get_required_vacation_timeoff_policyschedule_enabled = rail.PythonOperator(
            task_id='get_required_vacation_timeoff_policyschedule_enabled',
            python_callable=get_vacation_timeoff_policyschedule,
            op_args=[config.adtalem_vacation_timeoffpolicy_schedule_existingusers_old,
                     '{{ dag_run.conf.policyname }}']
        )

        put_vacation_policy_enabled = rail.RepliconServiceOperator(
            task_id='put_vacation_policy_enabled',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: get_put_vacation_policy_enabled(dag_run.conf['useruri'],
                                                                 config.first_timeofftype_in_instance,
                                                                 dag_run.conf['servicedate'],
                                                                 dag_run.conf['policyname'],
                                                                 dag_run.conf['previouspolicy'],
                                                                 'get_required_vacation_timeoff_policyschedule_enabled')
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_required_vacation_timeoff_policyschedule >> is_userstatus_disabled

        is_userstatus_disabled >> rail.Label(
            'Yes') >> put_vacation_policy_rehire >> dagrun_log_to_sumo

        is_userstatus_disabled >> rail.Label(
            'No') >> get_required_vacation_timeoff_policyschedule_enabled >> \
            put_vacation_policy_enabled >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_vacationpolicy_dag)
