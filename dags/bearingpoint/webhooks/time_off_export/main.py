from datetime import timedelta
from pendulum import datetime
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description= "BearingPoint Time Off Import Master (Endpoint)",
        start_date= datetime(2024,12,18),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.bearingpoint_timeoff_import_bearer_token_variable)
        ]
    ) as dag:

        # NOTE: Further business logic should be done in a separate child dag
        # To child dag all the posted data will be passed in conf from master(current dag)
        # So, creating a new version of integration will be easier in the future

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

    return dag

rail.for_each_instance(create_main_dag)
