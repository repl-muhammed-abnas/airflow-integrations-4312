from datetime import timedelta
from pendulum import datetime
from airflow.models import Variable
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.project_master_dag_id,
        description= "Wipro Project sync master dag (Endpoint)",
        start_date= datetime(2025,6,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.wipro_project_task_allocation_bearer_token_variable)
        ]
    ) as dag:

        # NOTE: Further business logic should be done in a separate child dag
        # To child dag all the posted data will be passed in conf from master(current dag)
        # So, creating a new version of integration will be easier in the future

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

        rail.TriggerDagRunOperator(
            task_id = 'process_projectdata',
            trigger_dag_id= config.projects_child_dag_id,
            conf=lambda dag_run: {
                "project_data": [dag_run.conf['webhook']['data']['WbsPMDet']] if isinstance(
                dag_run.conf['webhook']['data']['WbsPMDet'], (dict)) else dag_run.conf['webhook']['data']['WbsPMDet']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

    return dag

rail.for_each_instance(create_main_dag)
