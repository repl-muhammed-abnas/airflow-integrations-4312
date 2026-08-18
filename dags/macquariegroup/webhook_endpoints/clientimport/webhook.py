from datetime import timedelta
from pendulum import datetime, now
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'macquarie_clientimport_webhook_trigger_{config.instance}',
        description=f'Macquarie Client Import Webhook Master Trigger {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 7, 1, tz=config.timezone),
        max_active_runs=config.master_max_active_run,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.hmac_secret_var),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        log_current_time = rail.PythonOperator(
            task_id='log_current_time',
            python_callable=lambda: now(
                config.timezone).strftime('%Y-%m-%d-%H%M%S')
        )

        trigger_macquarie_process_clientimport_master = rail.TriggerDagRunOperator(
            task_id='trigger_macquarie_process_clientimport_master',
            retries=0,
            trigger_dag_id=config.master_ondemand_trigger_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={}
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        log_current_time >> rail.Label(
            'Yes') >> trigger_macquarie_process_clientimport_master >> finish >> log_dagrun_to_sumo
    return dag


rail.for_each_instance(create_dag)
