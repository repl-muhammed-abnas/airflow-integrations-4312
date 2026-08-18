from datetime import timedelta
import json
from airflow.models import Variable
from pendulum import datetime
import rail

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.tempo_token_generation_dag_id,
        description=f"iPipeline TEMPO (Within Jira) token generation (OAUTH) master dag {config.instance}",
        start_date=datetime(2026, 1, 1, tz=config.time_zone),
        schedule_interval=timedelta(
            days=config.tempo_token_generation_schedule_days),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_runs
    ) as dag:

        get_access_token = rail.SimpleHttpOperator(
            task_id='get_access_token',
            method='POST',
            http_conn_id=config.http_conn_id_tempo_token_generation,
            # Endpoint for token generation: https://api.tempo.io/oauth/token
            endpoint='',
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={
                "grant_type": "refresh_token",
                "client_id": f"{OPEN_BRACKETS}var.json.{config.tempo_client_id_secret_var}.client_id {CLOSE_BRACKETS}",
                "client_secret": f"{OPEN_BRACKETS}var.json.{config.tempo_client_id_secret_var}.client_secret {CLOSE_BRACKETS}",
                "refresh_token": f"{OPEN_BRACKETS}var.value.{config.tempo_refresh_token_var} {CLOSE_BRACKETS}",
                "redirect_uri": f"{OPEN_BRACKETS}var.json.{config.tempo_client_id_secret_var}.redirect_uri {CLOSE_BRACKETS}",
            }
        )

        set_access_token = rail.PythonOperator(
            task_id="set_access_token",
            python_callable=lambda: Variable.set(config.tempo_bearer_token_var, json.loads(
                rail.result("get_access_token"))["access_token"])
        )

        set_refresh_token = rail.PythonOperator(
            task_id="set_refresh_token",
            python_callable=lambda: Variable.set(config.tempo_refresh_token_var, json.loads(
                rail.result("get_access_token"))["refresh_token"])
        )

        get_access_token >> set_access_token >> set_refresh_token

        return dag


rail.for_each_instance(create_main_airflow_dag)
