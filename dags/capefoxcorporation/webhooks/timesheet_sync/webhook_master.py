from datetime import timedelta
import rail


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dag_id,
        description=f'Capefoxcorporation Timesheet Sync Webhook ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.webhook_secret_var)
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.TriggerDagRunOperator(
            task_id='trigger_timesheet_sync',
            trigger_dag_id=config.timesheet_sync_master_dag_id,
            conf=lambda dag_run: {
                **dag_run.conf
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

    return dag


rail.for_each_instance(create_main_dag)
