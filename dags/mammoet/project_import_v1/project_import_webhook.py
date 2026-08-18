from datetime import timedelta
from pendulum import datetime
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.project_master_dag_id,
        description= "Mammoet Project Import Master",
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.mammoet_project_bearer_token_variable)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

        rail.TriggerDagRunOperator(
            task_id = 'process_projects',
            trigger_dag_id= config.projects_child_dag_id,
            conf= lambda dag_run: {
                    "project_data": dag_run.conf['webhook']['data'],
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

    return dag

rail.for_each_instance(create_main_dag)
