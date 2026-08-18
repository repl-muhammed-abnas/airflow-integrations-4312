# pylint: disable=line-too-long
# pylint: disable=unused-import
from ce_replicon_integration.initial_setup.instances.production_us_east_1 import oefs as initial_setup_oefs, groups
from ce_replicon_integration.user_sync.config import *
region = 'us-east-1'
environment = 'production'
instance = 'production'
company_key = f"airflow{region.replace('-', '')}"
replicon_conn_id = 'airflow-replicon-admin'

hmac_secret = 'airflow_connector_ui_hmac_secret'

employee_last_sync_time_var = f'ce_replicon_employee_sync_last_sync_time_{instance}'
execution_timeout_days = 14
max_active_runs = 5
child_dag_max_active_runs = 10

user_sync_main_dag_id = f"standard_computerease_{region.replace('-', '_')}_user_sync_main_{instance}"
process_each_user_child_dag_id = f"standard_computerease_{region.replace('-', '_')}_process_each_user_child_dag_{instance}"

workflow = 'user_sync'
provider = 'computerease'

# Connector UI configuration for DAG run history logging
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'

oefs = list(filter(lambda oef: 'user' in oef['bind'], initial_setup_oefs))
