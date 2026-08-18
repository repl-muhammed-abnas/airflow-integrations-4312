from datetime import timedelta
from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dagid,
        description="Mammoet TimeOff Booking Import Webhook Master",
        start_date=datetime(2023, 10, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_wehook_master,
        webhook_conf=[
            rail.WebhookConf(
                bearer_token_var=config.mammoet_timeoff_booking_import_bearer_token_var)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        trigger_timeoff_import_processing_dag = rail.TriggerDagRunOperator(
            task_id="trigger_timeoff_import_processing_dag",
            trigger_dag_id=config.process_timeoff_import_payload_dagid,
            conf=lambda dag_run: {
                "payload": dag_run.conf['webhook']['data'],
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=lambda dag_run:{
                "count_of_records": len(dag_run.conf['webhook']['data'])
            }
        )

        trigger_timeoff_import_processing_dag >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
