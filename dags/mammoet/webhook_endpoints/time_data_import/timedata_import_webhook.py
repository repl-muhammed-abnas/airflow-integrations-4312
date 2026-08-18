from datetime import timedelta
from pendulum import datetime
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.timedata_master_dag_id,
        description= "mammoet timedata Import Master",
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.mammoet_timedata_bearer_token_variable)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

        process_timedata = rail.TriggerDagRunOperator(
            task_id = 'process_timedata',
            trigger_dag_id= config.timedata_child_dag_id,
            conf= lambda dag_run: {
                    "time_data": dag_run.conf['webhook']['data'],
                    "master_ecid": dag_run.conf['_ecid']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info= lambda dag_run: {
                'count_of_total_records': len(dag_run.conf['webhook']['data']['timedata'])
            }
        )

        process_timedata >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
