from ce_replicon_integration.project_sync.config import *
region = 'us-east-1'
environment = 'production'
instance = 'production'
company_key = f"airflow{region.replace('-', '')}"
replicon_conn_id = 'airflow-replicon-admin'

hmac_secret = 'airflow_connector_ui_hmac_secret'
job_last_sync_time_var = f'ce_replicon_job_sync_last_sync_time_{instance}'
execution_timeout_days = 14
child_dag_max_active_runs = 10
max_active_runs = 5

workflow = 'project_sync'
provider = 'computerease'

job_main_dag_id = f"standard_computerease_{region.replace('-', '_')}_job_sync_main_{instance}"
job_child_dag_id = f"standard_computerease_{region.replace('-', '_')}_job_sync_child_{instance}"
phases_child_dag_id = f"standard_computerease_{region.replace('-', '_')}_phases_sync_child_{instance}"
category_child_dag_id = f"standard_computerease_{region.replace('-', '_')}_category_sync_child_{instance}"

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'