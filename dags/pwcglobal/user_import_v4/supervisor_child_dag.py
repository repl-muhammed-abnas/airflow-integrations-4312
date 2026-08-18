from datetime import timedelta
import rail
from pwcglobal.user_import_v4.task.update_supervisor import get_update_supervisor

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import_v4/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.supervisor_dag_id,
        description=f'PwCGlobal_User_Import supervisor_assignment',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.supervisor_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_supervisor',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        supervisor_task_group = get_update_supervisor(
            "{{ dag_run.conf.useruri }}", can_queue_assignment=False)

        batch_task >> supervisor_task_group[0] >> finish
        batch_task >> finish

    return dag


rail.for_each_instance(create_dag)
