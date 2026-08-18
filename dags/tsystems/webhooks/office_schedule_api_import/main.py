from datetime import datetime, timedelta
import rail
import json

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.webhook_main_dag_id,
        description='Tsystems Office Schedule Sync API Import Webhook Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:
        
        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        def get_webhook_data(webhook_data):
            if isinstance(webhook_data, str):
                return json.loads(webhook_data)
            return webhook_data

        trigger_office_schedule_sync_api_master_dag = rail.TriggerDagRunOperator(
            task_id = 'trigger_office_schedule_sync_api_master_dag',
            trigger_dag_id= config.trigger_master_dag_id,
            conf= lambda dag_run: {
                    "payload": get_webhook_data(dag_run.conf['webhook']['data'])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        view_dagrun_config >> trigger_office_schedule_sync_api_master_dag

    return dag


rail.for_each_instance(create_main_dag)
