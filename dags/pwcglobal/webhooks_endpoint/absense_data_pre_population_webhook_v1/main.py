from datetime import timedelta,datetime as dt
from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_dag_id,
        description=f'PWC Absense Data Pre-population Webhook Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
        start_date=dt(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        rail.TriggerDagRunOperator(
            task_id="trigger_timesheetprepop_master_processing_dag",
            trigger_dag_id=config.trigger_master_dag_id,
            conf=lambda dag_run: {
                "webhook": dag_run.conf['webhook']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        ) 

    return dag

rail.for_each_instance(create_main_dag)
