# pylint: disable=wildcard-import unused-wildcard-import
from bamboohr.main_dag.config import *

instance = 'trial'

region = 'eu-central-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowsandbox-replicon-admin'

timezone_iana = 'Europe/Paris'

can_run_batch_task_var_name = f'standard_bamboohr_main_dag_{instance}_can_run_batch_task'

user_import_dag = f"standard_bamboohr_{region.replace('-', '_')}_user_import_{instance}"
disable_user_dag = f"standard_bamboohr_{region.replace('-', '_')}_disable_user_{instance}"

# This is only for the UI endpoint of the connector as workaround for connector specific testing
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_bamboohr'
