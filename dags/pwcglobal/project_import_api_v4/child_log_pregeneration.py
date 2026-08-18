import rail
from pwcglobal.project_import_api_v4.custom_method import do_format_logs


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_api_v4/config.py


# pylint:disable = too-many-statements
def create_child_process_log_pregeneration(config):
    with rail.create_airflow_dag(
        dag_id=config.project_import_api_log_generation_child_dag_id,
        description=f'Log Pregeneration {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_log_generation_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=do_format_logs
        )

        format_logs

        return dag


rail.for_each_instance(create_child_process_log_pregeneration)
