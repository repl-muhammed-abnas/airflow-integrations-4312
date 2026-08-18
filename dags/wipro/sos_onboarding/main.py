from uuid import uuid4
from pendulum import datetime
import rail
from airflow import DAG
from rail.lib.alerts_email import send_dagrun_alert_email

def create_main_dag(config):
    with DAG(
        dag_id= f"wipro_onboarding_api_test_dags_{config.instance}",
        description="Wipro Endpoint Test Dags",
        schedule=None, # this will be triggered manually
        start_date=datetime(2023,1,1),
        default_view='graph',
        user_defined_macros=rail.dag.get_macros(),
        user_defined_filters=rail.dag.get_filters(),
        max_active_runs=config.master_max_active_run,
        on_failure_callback=send_dagrun_alert_email,
        default_args={
            "owner": config.owner
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id ="view_dag_run_conf")

        token_generation = rail.SimpleHttpOperator(
            task_id = "token_generation",
            http_conn_id="wipro_http_token_generation",
            endpoint="{{ dag_run.conf.token_generation.endpoint }}",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
            data={"grant_type":"client_credentials"},
        )

        check_connectivity_to_sos = rail.HTTPUploadFileOperator(
            task_id = "check_connectivity_to_sos",
            http_conn_id="{{ dag_run.conf.check_connectivity_to_sos.http_conn_id }}",
            content_type= 'application/json',
            method="POST",
            endpoint="{{dag_run.conf.check_connectivity_to_sos.endpoint}}",
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer " +"{{ result('token_generation') | from_json | attr_or_default('access_token', 'none') }}"
            },
            content={}
        )

        check_connection_for_encryption = rail.HTTPUploadFileOperator(
            task_id = "check_connection_for_encryption",
            http_conn_id="{{dag_run.conf.check_connection_for_encryption.http_conn_id}}",
            content_type= 'text/plain',
            method='POST',
            endpoint="{{dag_run.conf.check_connection_for_encryption.endpoint}}",
            headers={
                "Authorization": "Bearer " +"{{ result('token_generation') | from_json | attr_or_default('access_token', 'none') }}",
                "AccessToken": "{{var.value.check_connection_for_encryption_access_token}}",
                "CorrelationId": str(uuid4())
            },
            content="{{dag_run.conf.check_connection_for_encryption.endpoint_payload}}"
        )

        sap_authentication_validation = rail.HTTPUploadFileOperator(
            task_id = "sap_authentication_validation",
            http_conn_id="{{dag_run.conf.sap_authentication_validation.http_conn_id}}",
            method='POST',
            endpoint="{{dag_run.conf.sap_authentication_validation.endpoint}}",
            content_type='text/plain',
            headers={
                "Authorization": "Bearer " +"{{ result('token_generation') | from_json | attr_or_default('access_token', 'none') }}",
                "AccessToken": "{{var.value.sap_authentication_validation_access_token}}",
                "CorrelationId": str(uuid4())
            },
            content="{{dag_run.conf.sap_authentication_validation.endpoint_payload}}"
        )

        token_generation >> [check_connectivity_to_sos, check_connection_for_encryption, sap_authentication_validation]

    return dag

rail.for_each_instance(create_main_dag)
