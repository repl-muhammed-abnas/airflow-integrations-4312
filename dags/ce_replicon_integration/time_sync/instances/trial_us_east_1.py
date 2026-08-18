# pylint: disable=line-too-long
# pylint: disable=unused-import
from ce_replicon_integration.time_sync.config import *
from ce_replicon_integration.initial_setup.instances.trial_us_east_1 import oefs as initial_setup_oefs, replicon_export_file_format_name
region = 'us-east-1'
environment = 'pre-production'
instance = 'trial'
company_key = f"airflowsandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowsandbox-replicon-admin'

hmac_secret = 'airflow_connector_ui_hmac_secret'
time_data_last_sync_time_var = f'ce_replicon_job_sync_last_sync_time_{instance}'
execution_timeout_days = 14
max_active_runs = 5
child_max_active_runs = 10

workflow = 'time_sync'
provider = 'computerease'

main_dag_id = f"standard_computerease_{region.replace('-', '_')}_time_sync_main_{instance}"
child_dag_id = f"standard_computerease_{region.replace('-', '_')}_time_sync_per_employee_child_{instance}"

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_computerease'

oefs = list(filter(lambda oef: 'timesheet' in oef.get('bind', []), initial_setup_oefs))
