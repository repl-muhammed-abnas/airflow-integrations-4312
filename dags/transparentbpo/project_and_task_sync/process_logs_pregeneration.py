import rail
from datetime import timedelta
from transparentbpo.project_and_task_sync.utils import python_callable


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_logs_pregeneration_dag_id,
        description='Transparentbpo - Project & Task sync Process Logs - Pregeneration',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_logs_pregeneration,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")
        
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.do_format_logs,
            show_return_value_in_logs=False
        )

    return dag


rail.for_each_instance(create_main_airflow_dag)
