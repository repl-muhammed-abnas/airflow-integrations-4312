import rail
from pendulum import datetime


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.dag_id,
        description=config.dag_description,
        company_key=config.company_key,
        replicon_conn_id = config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.webhook_secret)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

    return dag

rail.for_each_instance(create_dag)
