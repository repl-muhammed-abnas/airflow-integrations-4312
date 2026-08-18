from datetime import datetime
import json
import rail

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'grantthorntonadvisorsllc_user_import_webhook_{config.instance}',
        description='grantthorntonadvisorsllc_user_import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2026, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            response_data_task_id="validate_response",
            bearer_token_var=config.bearer_token_var)
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        validate_response = rail.PythonOperator(
            task_id="validate_response",
            python_callable=lambda dag_run: json.dumps({
            "validationResponse": dag_run.conf.get("webhook", {}).get("data", [{}])[0].get("data", {}).get("validationCode")\
                       if dag_run.conf.get("webhook", {}).get("data") else ""
            })
        )

    return dag


rail.for_each_instance(create_main_dag)
