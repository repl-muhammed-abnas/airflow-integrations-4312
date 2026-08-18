from pendulum import datetime
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.user_import_master_dag_id,
        description= "Mammoet User Import Master API Endpoint",
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.mammoet_user_import_bearer_token_variable)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

        trigger_process_payload_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_process_payload_dag",
            trigger_dag_id=config.user_import_process_payload_child_dag_id,
            retries = 0,
            conf=lambda dag_run: {
                "payload_id": dag_run.conf['webhook']['data']['payload_identifier'],
                "users_data": dag_run.conf['webhook']['data']['users']
            }
        )

        def get_extra_info(dag_run):
            return {
                "payload_id": dag_run.conf['webhook']['data']['payload_identifier'],
                "user_count": len(dag_run.conf['webhook']['data']['users'])
            }

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info= get_extra_info
        )

        trigger_process_payload_dag >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
