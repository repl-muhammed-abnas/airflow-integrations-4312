from datetime import datetime, timedelta
import rail

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.project_sync_webhook_main_dag,
        description='GroupMPortugal Project Sync Webhook Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.TriggerDagRunOperator(
            task_id = 'trigger_project_sync_master_dag',
            trigger_dag_id= config.project_sync_master_dag_id,
            conf= lambda dag_run: {
                    "payload": dag_run.conf['webhook']['data']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

    return dag


rail.for_each_instance(create_main_dag)
