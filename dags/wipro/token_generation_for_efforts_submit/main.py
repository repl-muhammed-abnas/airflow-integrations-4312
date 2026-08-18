import json
from airflow.models import Variable
from pendulum import datetime
import rail


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"wipro_token_generation_for_efforts_submission_master_{config.instance}",
        description="Wipro token generation for efforts submission master dag (Endpoint)",
        start_date=datetime(2023, 9, 29, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_runs
    ) as dag:

        get_wipro_auth_token = rail.SimpleHttpOperator(
            task_id="get_wipro_auth_token",
            http_conn_id="wipro_http_token_generation",
            endpoint="oauth2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
            data={"grant_type": "client_credentials"},
        )

        set_token = rail.PythonOperator(
            task_id="set_token",
            python_callable=lambda: Variable.set(config.wipro_efforts_submission_bearer_token_variable,
                                                 json.loads(rail.result("get_wipro_auth_token"))["access_token"])
        )

        get_wipro_auth_token >> set_token

        return dag


rail.for_each_instance(create_main_airflow_dag)
