# pylint: disable=wildcard-import unused-wildcard-import
from wipro.webhooks.project_import.config import *

instance = "uat"

region = 'eu-central-1'
environment = "pre-production"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_repliconint"

wipro_timeoff_export_approved_access_token_variable = f"wipro_timeoff_export_approved_access_token_variable_{instance}_secret"
wipro_timeoff_export_rejected_access_token_variable = f"wipro_timeoff_export_rejected_access_token_variable_{instance}_secret"
wipro_timeoff_export_waiting_access_token_variable = f"wipro_timeoff_export_waiting_access_token_variable_{instance}_secret"
wipro_timeoff_export_deleted_access_token_variable = f"wipro_timeoff_export_deleted_access_token_variable_{instance}_secret"
master_dag_id = f"wipro_time_off_export_master_{instance}"
child_dag_id = f"wipro_time_off_export_process_payload_child_{instance}"

disabled=True
