from datetime import timedelta
from pendulum import datetime
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description= "Raynetsas User Import Master",
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.raynetsas_user_import_bearer_token_variable)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

        trigger_user_import_processing_dag = rail.TriggerDagRunOperator(
            task_id="trigger_user_import_processing_dag",
            trigger_dag_id=config.process_user_import_payload_dagid,
            conf=lambda dag_run: {
                "payload": dag_run.conf['webhook']['data']['users']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=lambda dag_run:{
                "count_of_user_records": len(dag_run.conf['webhook']['data']['users'])
            }
        )

        trigger_user_import_processing_dag >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
