from datetime import datetime
import rail

def create_main_dag(config):
    
    with rail.create_airflow_dag(
        dag_id=config.webhook_main_dag_id,
        description='KPMG_au_userimport',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 6, 1),
        max_active_runs=config.max_active_runs_webhook,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:
        
        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        view_dagrun_config

    return dag

rail.for_each_instance(create_main_dag)
