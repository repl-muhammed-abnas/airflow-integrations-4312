from jira.create_user.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'
company_key = f"airflow{region.replace('-', '')}"
hmac_secret = 'airflow_connector_ui_hmac_secret'
replicon_conn_id = 'airflow-replicon-admin'
can_run_batch_task_var_name = f'standard_jira_create_user_{instance}_can_run_batch_task'
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'

s3_reference_prefix = f'{s3_folder}/{environment}/'
s3_all_users_reference_key = f'{s3_reference_prefix}{s3_all_users_reference_file}'
s3_role_based_reference_key = f'{s3_reference_prefix}{s3_role_based_reference_file}'
s3_group_based_reference_key = f'{s3_reference_prefix}{s3_group_based_reference_file}'
