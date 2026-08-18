"""
T-Systems Germany/Iberia Time Off Import Webhook DAG
Receives time off data from SAP BTP and triggers the main import process
"""

from datetime import datetime, timedelta
import json
import rail

def create_main_dag(config):
    """
    Creates webhook DAG for receiving time off data from SAP BTP layer
    """
    with rail.create_airflow_dag(
        dag_id=config.webhook_main_dag_id,
        description='T-Systems Germany/Iberia Time Off Import Webhook - Receives JSON from SAP BTP',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 6, 1),
        max_active_runs=config.max_active_runs_webhook,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:
        
        # View the incoming webhook payload
        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        def get_webhook_data(webhook_data):
            if isinstance(webhook_data, str):
                return json.loads(webhook_data)
            return webhook_data

        # Trigger the master import DAG with the webhook data
        trigger_import_dag = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_import_master_dag',
            trigger_dag_id=config.trigger_master_dag_id,
            conf=lambda dag_run: {
                "webhook_data":  get_webhook_data(dag_run.conf['webhook']['data']),
                "received_at": datetime.now().isoformat()
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        # Log webhook processing to Sumologic
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Define task dependencies
        view_dagrun_config >> trigger_import_dag >> log_to_sumo

    return dag

# Create DAG for each configured instance
rail.for_each_instance(create_main_dag)
