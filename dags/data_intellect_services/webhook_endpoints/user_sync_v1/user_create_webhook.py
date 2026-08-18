import json
from pendulum import datetime
from data_intellect_services.webhook_endpoints.user_sync_v1.utils import python_callable
import rail

null = None

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"data_intellect_user_import_create_webhook_{config.instance}_v1",
        description=f"Data intellect services user sync create webhook dag (Endpoint) {config.instance} V1",
        start_date=datetime(2023, 9, 1, tz=config.time_zone),
        schedule_interval=None,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.webhook_create_max_active_runs,
        webhook_conf=[
            rail.WebhookConf(
                hmac_secret_var=config.data_intellect_hmac_shared_secret_user_create)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        create_tenant_wide_log = rail.CreateLogOperator(
            task_id='create_tenant_wide_log',
            tenant_wide_name=config.user_sync_tenant_wide_log_name,
            existing_log_mode="append"
        )

        get_user_details_from_hibob = rail.SimpleHttpOperator(
            task_id='get_user_details_from_hibob',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint="people/{{ dag_run.conf.webhook.data.employee.id }}",
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            data=json.dumps({
                "humanReadable": "REPLACE"
            }),
            response_filter=lambda response: json.loads(response.text) if json.loads(response.text) else null
        )

        log_create_payload = rail.WriteLogOperator(
            task_id='log_create_payload',
            log='{{ result("create_tenant_wide_log") }}',
            message="Create payload is logged",
            severity="Success",
            properties=python_callable.log_create_payload
        )

        create_tenant_wide_log >> get_user_details_from_hibob >> log_create_payload

    return dag

rail.for_each_instance(create_main_dag)
