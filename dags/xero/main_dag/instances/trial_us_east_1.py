# pylint: disable=wildcard-import unused-wildcard-import
from xero.main_dag.config import *

instance = 'trial'

region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowsandbox-replicon-admin'

timezone_iana = 'America/Los_Angeles'

can_run_batch_task_var_name = f'standard_xero_main_dag_{instance}_can_run_batch_task'

client_import_dag = f"standard_xero_connector_{region.replace('-', '_')}_client_import_{instance}"
client_export_dag = f"standard_xero_connector_{region.replace('-', '_')}_client_export_{instance}"
invoice_export_dag = f"standard_xero_connector_{region.replace('-', '_')}_invoice_export_{instance}"
billed_status_update_dag = f"standard_xero_connector_{region.replace('-', '_')}_invoice_status_update_billed_{instance}"
paid_status_update_dag = f"standard_xero_connector_{region.replace('-', '_')}_invoice_status_update_paid_{instance}"

# This is only for the UI endpoint of the connector as workaround for connector specific testing
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_xero'
