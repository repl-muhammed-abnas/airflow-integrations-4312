# pylint: disable=wildcard-import unused-wildcard-import
from jira.main_dag.config import *

instance = 'trial'

region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowsandbox-replicon-admin'

timezone_iana = 'America/Los_Angeles'

can_run_batch_task_var_name = f'standard_jira_main_dag_{instance}_can_run_batch_task'

close_task_dag = f"standard_jira_{region.replace('-', '_')}_close_task_{instance}"
create_task_dag = f"standard_jira_{region.replace('-', '_')}_create_task_{instance}"
project_import_dag = f"standard_jira_{region.replace('-', '_')}_project_import_{instance}"
user_export_dag = f"standard_jira_{region.replace('-', '_')}_user_export_{instance}"
create_user_dag = f"standard_jira_{region.replace('-', '_')}_create_user_{instance}"

# This is only for the UI endpoint of the connector as workaround for connector specific testing
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_jira'
