from pendulum import datetime
from rail.lib.ecid import get_dagrun_ecid
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description= "BearingPoint User Import Master (Endpoint)",
        start_date= datetime(2024,12,18),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.bearingpoint_user_import_bearer_token_variable)
        ]
    ) as dag:

        # NOTE: Further business logic should be done in a separate child dag
        # To child dag all the posted data will be passed in conf from master(current dag)
        # So, creating a new version of integration will be easier in the future

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

        rail.TriggerDagRunOperator(
            task_id="trigger_user_import_master",
            trigger_dag_id=config.process_payload_child_dag_id,
            conf=lambda dag_run: {
                **dag_run.conf,
                "master_ecid": get_dagrun_ecid(dag_run)
            }
        )

    return dag

rail.for_each_instance(create_main_dag)
