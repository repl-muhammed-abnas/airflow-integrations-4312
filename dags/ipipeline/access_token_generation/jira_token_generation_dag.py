from datetime import timedelta
import json
from airflow.models import Variable
from pendulum import datetime
import rail

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.jira_token_generation_dag_id,
        description=f"iPipeline JIRA token generation (OAUTH) master dag {config.instance}",
        start_date=datetime(2026, 1, 1, tz=config.time_zone),
        schedule_interval=timedelta(
            minutes=config.jira_token_generation_schedule_minutes),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_runs
    ) as dag:

        # Get access token for JIRA using Service Account credentials
        get_jira_access_token = rail.SimpleHttpOperator(
            task_id='get_jira_access_token',
            method='POST',
            http_conn_id=config.http_conn_id_jira_token_generation,
            # endpoint: https://api.atlassian.com/oauth/token
            endpoint='',
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={
                "grant_type": "client_credentials",
                "client_id": f"{OPEN_BRACKETS}var.json.{config.jira_client_id_secret_var}.client_id {CLOSE_BRACKETS}",
                "client_secret": f"{OPEN_BRACKETS}var.json.{config.jira_client_id_secret_var}.client_secret {CLOSE_BRACKETS}",
            }
        )
        
        set_jira_token = rail.PythonOperator(
            task_id="set_jira_token",
            python_callable=lambda: Variable.set(config.jira_bearer_token_var,
                json.loads(rail.result("get_jira_access_token"))["access_token"])
        )

        get_jira_access_token >> set_jira_token

        return dag


rail.for_each_instance(create_main_airflow_dag)
