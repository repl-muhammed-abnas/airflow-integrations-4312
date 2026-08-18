from datetime import datetime, timedelta
from pendulum import now
import rail
import json


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dagid,
        description='CRL Office Schedule Import Webhook Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 10, 1),
        max_active_runs=config.max_active_runs_wehook_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        
        def get_webhook_data(webhook_data):
            if isinstance(webhook_data, str):
                return json.loads(webhook_data)
            return webhook_data

        # Trigger the master import DAG with the webhook data
        trigger_import_dag = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_import_master_dag',
            trigger_dag_id=config.office_schedule_import_master_dag_id,
            conf=lambda dag_run: {
                "payload":  get_webhook_data(dag_run.conf['webhook']['data']),
                "received_at": now(config.time_zone).isoformat()
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
        trigger_import_dag >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
