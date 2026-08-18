from datetime import timedelta
from pendulum import datetime
from airflow.models import Variable
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.project_master_dag_id,
        description= "Wipro Project/task allocation import master dag (Endpoint)",
        start_date= datetime(2023,9,1),
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

        can_process_payload = rail.IfOperator(
            task_id='can_process_payload',
            test=lambda: Variable.get(
                config.can_process_payload_var).lower() == 'true',
            yes_task='process_projectdata'
        )

        process_projectdata = rail.TriggerDagRunOperator(
            task_id = 'process_projectdata',
            trigger_dag_id= config.projects_child_dag_id,
            conf=lambda dag_run: {
                "project_data" :[dag_run.conf['webhook']['data']['item']] if isinstance(
                dag_run.conf['webhook']['data']['item'], (dict)) else dag_run.conf['webhook']['data']['item']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        can_process_payload >> rail.Label(
            "Yes") >> process_projectdata

    return dag

rail.for_each_instance(create_main_dag)
