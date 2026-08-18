instance = "production"
region = "eu-central-1"
environment = "production"
execution_timeout_days = 14
company_key = f"airflow{region.replace('-', '')}"
airflow_connector_ui_connid = "airflow_connector_ui_endpoint"
hmac_secret = "airflow_connector_ui_hmac_secret"
replicon_conn_id = "airflow-replicon-admin"
can_run_batch_task_var_name = f"zendesk_project_import_{instance}_can_run_batch_task"

max_active_runs = 10

provider = "zendesk"
workflow = "project_import"
