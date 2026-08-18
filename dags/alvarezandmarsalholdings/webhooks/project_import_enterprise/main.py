from datetime import datetime, timedelta
from airflow.models import Variable
import rail

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.project_import_enterprise_webhook_main_dag,
        description='Alvarez and Marsal Holdings Project Import Enterprise Webhook Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def get_trigger_id():
            trigger_id = Variable.get(config.trigger_dag_id_var, default_var='')
            trigger_id = trigger_id if trigger_id else config.project_master_dag
            return trigger_id
        
        rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_enterprise_projects',
            items=[0],
            trigger_dag_id= get_trigger_id,
            conf= lambda dag_run: {
                    "payload": dag_run.conf['webhook']['data']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda dag_run: {
                "Total Number of Records": len(dag_run.conf['webhook']['data'].get('A_EnterpriseProjects', [])) if dag_run.conf['webhook']['data'] else 0
            },
        )

    return dag


rail.for_each_instance(create_main_dag)
