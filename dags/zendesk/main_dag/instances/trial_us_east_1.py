# pylint: disable=wildcard-import unused-wildcard-import
from zendesk.main_dag.config import *

instance = "trial"

region = "us-east-1"
environment = "pre-production"
company_key = f"airflowsandbox{region.replace('-', '')}"
replicon_conn_id = "airflowsandbox-replicon-admin"

timezone_iana = "Europe/Paris"

can_run_batch_task_var_name = f"standard_zendesk_main_dag_{instance}_can_run_batch_task"

client_import_dag = f"standard_zendesk_connector_{region.replace('-', '_')}_create_updated_client_import_master_{instance}"
project_import_dag = f"standard_zendesk_connector_{region.replace('-', '_')}_create_updated_projects_import_master_{instance}"

airflow_connector_ui_connid = "airflow_connector_ui_endpoint_zendesk"
