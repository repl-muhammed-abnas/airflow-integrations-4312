from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/dominos/user_import/config.py


def create_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dominos_userimport_child_process_user_{config.instance}',
        description=f'Dominos_Child_Process User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.updateuser_child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_userlog'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_userlog',
            end_task='dagrun_log_to_sumo'
        )

        create_userlog = rail.CreateLogOperator(
            task_id='create_userlog'
        )

        is_loginname_present = rail.IfOperator(
            task_id='is_loginname_present',
            test="{{ dag_run.conf | attr_or_default('loginname') | sn | \
                is_truthy }}",
            yes_task="process_useruri",
            no_task="dagrun_log_to_sumo"
        )

        process_useruri = rail.EmptyOperator(
            task_id='process_useruri'
        )

        is_useruri_present = rail.IfOperator(
            task_id='is_useruri_present',
            test="{{ dag_run.conf | attr_or_default('useruri') | sn | \
                is_truthy }}",
            yes_task="trigger_updateuser_child_dag",
            no_task="write_useruri_exception",
        )

        trigger_updateuser_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_updateuser_child_dag',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'dominos_userimport_child_update_user_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda item: {
                **{
                    k: v for k, v in item.items() if k not in ('enddate', 'supervisorid', '_ancestry', '_ecid', '_replication_position')
                },
                'enddate': item['enddate'].replace('-', '/') if item['enddate'] else '',
                'supervisorid': item['supervisorid'].lower() if item['supervisorid'] else '',
                'log': rail.result('create_userlog')
            }
        )

        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user',
            dag_runs="{{ result('trigger_updateuser_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        write_useruri_exception = rail.WriteLogOperator(
            task_id='write_useruri_exception',
            log="{{ result('create_userlog') }}",
            message='Ignored | User not available in Replicon',
            severity='Ignored',
            properties={
                'loginname': '{{ dag_run.conf.loginname }}',
                'status': 'Ignored',
                'reason': 'Ignored | User not available in Replicon'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> create_userlog

        create_userlog >> is_loginname_present

        is_loginname_present >> rail.Label(
            'Yes') >> process_useruri >> is_useruri_present

        is_useruri_present >> rail.Label(
            'Yes') >> trigger_updateuser_child_dag >> wait_for_update_user >> \
            dagrun_log_to_sumo

        is_useruri_present >> rail.Label(
            'No') >> write_useruri_exception >> dagrun_log_to_sumo

        is_loginname_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_user_child_dag)
