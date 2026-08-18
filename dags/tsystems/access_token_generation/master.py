import json
from airflow.models import Variable
from pendulum import datetime
import rail

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description="T-Systems token generation master dag (Endpoint)",
        start_date=datetime(2025, 8, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_runs
    ) as dag:

        get_access_token = rail.SimpleHttpOperator(
            task_id='get_access_token',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='/auth/realms/default/protocol/openid-connect/token',
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={
                "grant_type": "client_credentials",
                "client_id": f"{OPEN_BRACKETS}var.json.{config.client_id_secret_variable_name}.client_id {CLOSE_BRACKETS}",
                "client_secret": f"{OPEN_BRACKETS}var.json.{config.client_id_secret_variable_name}.client_secret {CLOSE_BRACKETS}",
            }
        )

        set_token = rail.PythonOperator(
            task_id="set_token",
            python_callable=lambda: Variable.set(config.token_var,
                json.loads(rail.result("get_access_token"))["access_token"])
        )

        get_access_token >> set_token

        return dag


rail.for_each_instance(create_main_airflow_dag)
