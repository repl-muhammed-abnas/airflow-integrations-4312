from datetime import timedelta
from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dagid,
        description="Eisner Amper Project Import Add Customer - Webhook Master",
        start_date=datetime(2023, 12, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_wehook_master,
        webhook_conf=[
            rail.WebhookConf(
                bearer_token_var=config.bearer_token_var)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        rail.TriggerDagRunOperator(
            task_id="trigger_project_import_processing_dag",
            trigger_dag_id=config.process_project_import_payload_dagid,
            conf=lambda dag_run: {
                "payload": dag_run.conf['webhook']['data']['ProjectSet'],
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

    return dag

rail.for_each_instance(create_main_dag)
