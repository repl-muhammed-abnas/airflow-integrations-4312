from datetime import datetime
import rail

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.project_import_internal_webhook_main_dag,
        description='Alvarez and Marsal Holdings Project Import Internal Webhook Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

    return dag


rail.for_each_instance(create_main_dag)
