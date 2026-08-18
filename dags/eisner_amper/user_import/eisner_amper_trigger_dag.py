from datetime import datetime
import rail


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description='Eisner Amper User Import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        process_user_data = rail.TriggerDagRunOperator(
            task_id='process_user_data',
            trigger_dag_id=config.user_sync_child_dag_id,
            conf=lambda dag_run: dag_run.conf
        )

        return dag


rail.for_each_instance(create_main_airflow_dag)
