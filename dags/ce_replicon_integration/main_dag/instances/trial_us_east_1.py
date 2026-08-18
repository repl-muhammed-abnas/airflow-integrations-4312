from ce_replicon_integration.main_dag.config import *
region = 'us-east-1'
environment = 'pre-production'
instance = 'trial'
company_key = f"airflowsandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowsandbox-replicon-admin'
timezone_iana = 'America/New_York'

can_run_batch_task_var_name = f'standard_computerease_main_dag_{instance}_can_run_batch_task'
initial_setup_last_run_var = f'ce_replicon_initial_setup_last_run_{instance}'

initial_setup_dag = f"standard_computerease_{region.replace('-', '_')}_initial_setup_main_{instance}"
user_sync_dag = f"standard_computerease_{region.replace('-', '_')}_user_sync_main_{instance}"
project_sync_dag = f"standard_computerease_{region.replace('-', '_')}_job_sync_main_{instance}"
time_sync_dag = f"standard_computerease_{region.replace('-', '_')}_time_sync_main_{instance}"

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_computerease'