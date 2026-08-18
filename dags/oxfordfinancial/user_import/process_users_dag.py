from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/user_import/config.py


def create_processuser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_user_import_process_users_{config.instance}',
        description=f'Process User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_action_disable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_action_disable',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        is_action_disable = rail.IfOperator(
            task_id='is_action_disable',
            test="{{ dag_run.conf.action == 'disable' }}",
            yes_task="create_disableuser_log",
            no_task="create_addupdateuser_log"
        )

        create_disableuser_log = rail.CreateLogOperator(
            task_id='create_disableuser_log'
        )

        trigger_disableuser_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_disableuser_dag',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'oxfordfinancial_user_import_disable_users_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{k: v for k, v in item.items() if k not in (
                    '_ancestry', '_ecid', '_replication_position')},
                **{
                    'log': rail.result('create_disableuser_log')
                }}
        )

        wait_for_disableuser_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_disableuser_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_disableuser_dag") }}'
        )

        create_addupdateuser_log = rail.CreateLogOperator(
            task_id='create_addupdateuser_log'
        )

        is_update_user = rail.IfOperator(
            task_id='is_update_user',
            test="{{ dag_run.conf.useruri | is_truthy }}",
            yes_task="get_user_details",
            no_task="trigger_adduser_dag"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        is_user_enabled = rail.IfOperator(
            task_id='is_user_enabled',
            test="{{ result('get_user_details').isEnabled | is_truthy }}",
            yes_task="trigger_updateuser_dag",
            no_task="enable_user"
        )

        trigger_updateuser_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_updateuser_dag',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'oxfordfinancial_user_import_update_users_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{k: v for k, v in item.items() if k not in (
                    '_ancestry', '_ecid', '_replication_position')},
                **{
                    'log': rail.result('create_addupdateuser_log')
                }}
        )

        wait_for_updateuser_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_updateuser_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_updateuser_dag") }}'
        )

        enable_user = rail.RepliconServiceOperator(
            task_id='enable_user',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        trigger_adduser_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_adduser_dag',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'oxfordfinancial_user_import_create_users_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{k: v for k, v in item.items() if k not in ('useruri', 'current_department', '_ancestry',
                                                              '_ecid', '_replication_position')},
                **{
                    'log': rail.result('create_addupdateuser_log')
                }}
        )

        wait_for_adduser_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_adduser_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_adduser_dag") }}'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> is_action_disable
        is_action_disable >> rail.Label(
            'Yes') >> create_disableuser_log >> trigger_disableuser_dag >> wait_for_disableuser_dag >> dagrun_log_to_sumo
        is_action_disable >> rail.Label(
            'No') >> create_addupdateuser_log >> is_update_user
        is_update_user >> rail.Label(
            'Yes') >> get_user_details >> is_user_enabled
        is_user_enabled >> rail.Label(
            'Yes') >> trigger_updateuser_dag
        is_user_enabled >> rail.Label(
            'No') >> enable_user >> trigger_updateuser_dag
        trigger_updateuser_dag >> wait_for_updateuser_dag >> dagrun_log_to_sumo
        is_update_user >> rail.Label(
            'No') >> trigger_adduser_dag >> wait_for_adduser_dag >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_processuser_dag)
