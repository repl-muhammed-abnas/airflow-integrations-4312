from datetime import timedelta
from pendulum import datetime
from rail.lib.ecid import get_dagrun_ecid
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description= "BearingPoint Project Import Master (Endpoint)",
        start_date= datetime(2024,12,18),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.bearingpoint_project_import_bearer_token_variable)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

        rail.TriggerDagRunOperator(
            task_id = 'process_projects',
            trigger_dag_id= config.process_payload_dagid,
            conf= lambda dag_run: {
                **dag_run.conf['webhook']['data'][0],
                "master_ecid": get_dagrun_ecid(dag_run)
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

    return dag

rail.for_each_instance(create_main_dag)
