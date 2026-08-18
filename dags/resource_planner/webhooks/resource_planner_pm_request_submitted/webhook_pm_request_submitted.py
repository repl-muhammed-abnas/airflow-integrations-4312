from pendulum import datetime
import rail


def create_pm_request_submitted_webhook_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_dag_id,
        description="Webhook receiver for PM Request Submitted events from Resource Planner",
        start_date=datetime(2023, 9, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.webhook_bearer_token,
        ),
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        def _build_processing_conf(dag_run):
            data = ((dag_run.conf or {}).get('webhook') or {}).get('data')
            if not data:
                raise ValueError(
                    "Webhook payload has no 'webhook.data' — cannot trigger the "
                    f"PM request processor. Received keys: {list(dag_run.conf or {})}"
                )
            return data

        rail.TriggerDagRunOperator(
            task_id="trigger_processor",
            trigger_dag_id=config.pm_request_processor_dag_id,
            retries=0,
            conf=_build_processing_conf,
        )

    return dag


rail.for_each_instance(create_pm_request_submitted_webhook_dag)
