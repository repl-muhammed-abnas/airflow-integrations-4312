# pylint: disable=line-too-long
# pylint: disable=unused-import
from deltek_vantagepoint_v2.initial_setup.instances.production_eu_central_1 import oefs, groups
from deltek_vantagepoint_v2.user_sync.config import *
region = 'eu-central-1'
environment = 'production'
instance = "production"
company_key = f'airflow{region.replace("-", "")}'
replicon_conn_id = 'airflow-replicon-admin'

can_run_batch_task_var_name = f'vp_replicon_user_import_can_run_batch_task_{instance}'
user_sync_initial_run_var = f'vp_replicon_user_sync_initial_run_{instance}'

webhook_basicauth_username = f'deltek_vantagepoint_webhook_username_{company_key}'
webhook_basicauth_password = f'deltek_vantagepoint_webhook_password_{company_key}'

# DAG IDs
user_sync_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_sync_main_{instance}'
process_each_user_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_process_each_user_sync_child_{instance}'
supervisor_assignment_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_process_supervisor_assignment_child_{instance}'

oefs = list(filter(lambda oef: 'user' in oef['bind'], oefs))


sync_users_by_status = ['A']

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'
