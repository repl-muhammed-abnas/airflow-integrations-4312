from datetime import datetime
from airflow.models import Variable
import rail


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'eisner_amper_user_import{config.instance}',
        description='Eisner Amper User Import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_post_data_to_workato = rail.IfOperator(
            task_id='can_post_data_to_workato',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name).lower() == 'true',
            yes_task='post_to_workato',
            no_task='trigger_usersync_dag'
        )

        post_to_workato = rail.SimpleHttpOperator(
            task_id='post_to_workato',
            method='POST',
            http_conn_id=config.workato_api_endpoint,
            headers={
                "Content-Type": 'text/plain; charset=utf-8',
            },
            data="{{ dag_run.conf | to_json(ensure_ascii=True) }}",
        )

        trigger_usersync_dag = rail.TriggerDagRunOperator(
            task_id='trigger_usersync_dag',
            trigger_dag_id=config.master_dag,
            conf=lambda dag_run: dag_run.conf
        )

        can_post_data_to_workato >> rail.Label(
            'Yes') >> post_to_workato

        can_post_data_to_workato >> rail.Label(
            'No') >> trigger_usersync_dag

        return dag


rail.for_each_instance(create_main_airflow_dag)
