from datetime import datetime
import rail

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'replicon_test_sync_webhook_{config.instance}',
        description='replicon_test_sync_webhook',
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
