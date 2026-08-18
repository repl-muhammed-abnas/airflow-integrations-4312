from deltek_vantagepoint_v2.main_dag.config import *
region = 'us-east-1'
environment = 'qa'
instance = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowqasandbox-replicon-admin'
timezone_iana = 'America/New_York'

can_run_batch_task_var_name = f'deltek_vantagepoint_main_dag_{instance}_can_run_batch_task'
initial_setup_last_run_var = f'vp_replicon_initial_setup_last_run_{instance}'
timecategory_sync_last_run_var = f'vp_replicon_timecategory_sync_last_run_{instance}'
user_sync_initial_run_var = f'vp_replicon_user_sync_initial_run_{instance}'
is_project_full_sync_var = f'vp_replicon_is_project_full_sync_{instance}'

initial_setup_dag_id = f"standard_deltek_vantagepoint_{region.replace('-', '_')}_initial_setup_main_{instance}"
user_sync_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_sync_main_{instance}'
project_sync_main_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_project_sync_main_{instance}'
timesheet_sync_main_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_timesheet_sync_main_{instance}'
timecategory_sync_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_timecategory_sync_{instance}'

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_vantagepoint'
