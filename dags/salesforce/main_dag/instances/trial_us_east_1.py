# pylint: disable=wildcard-import unused-wildcard-import
from salesforce.main_dag.config import *

instance = 'trial'

region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowsandbox-replicon-admin'

timezone_iana = 'America/Los_Angeles'

can_run_batch_task_var_name = f'standard_salesforce_main_dag_{instance}_can_run_batch_task'

client_import_dag = f"standard_salesforce_{region.replace('-', '_')}_client_import_{instance}"
project_import_dag = f"standard_salesforce_{region.replace('-', '_')}_project_import_{instance}"

# This is only for the UI endpoint of the connector as workaround for connector specific testing
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_salesforce'
