# pylint: disable=wildcard-import unused-wildcard-import
from wcs.webhook.time_sync_to_quickbooks.config import *

instance = "trial"
company_key = "WCSafmig"

replicon_conn_id = f"{company_key}-replicon-admin"

# DAG IDs
master_dag_id = f"wcs_time_sync_to_quickbooks_webhook_master_{instance}"
process_timesheet_data_child_id = f"wcs_time_sync_to_quickbooks_time_sync_to_quickbooks_process_timesheet_data_child_{instance}"

# Token variable
wcs_time_sync_to_quickbooks_bearer_token_var = f"wcs_time_sync_to_quickbooks_bearer_token_variable_{instance}"

# Variable
lookup_log_timestamp_var = f"wcs_time_sync_to_quickbooks_lookup_log_timestamp_{instance}"
tenant_wide_log_var = f"wcs_time_sync_to_quickbooks_tenant_wide_log_{instance}"

# Email configuration
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"