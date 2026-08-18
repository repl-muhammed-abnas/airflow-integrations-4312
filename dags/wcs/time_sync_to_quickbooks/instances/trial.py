# pylint: disable=wildcard-import unused-wildcard-import
from wcs.time_sync_to_quickbooks.config import *
from wcs.time_sync_to_quickbooks.mapper.wcs_pay_item_reference_mapper_trial import wcs_pay_item_reference_mapper

# Instance
instance = "trial"
company_key = "WCSafmig"
replicon_conn_id = f"{company_key}-replicon-admin"

# DAG IDs
process_timesheet_data_child_id = f"wcs_time_sync_to_quickbooks_time_sync_to_quickbooks_process_timesheet_data_child_{instance}"
replicon_qbo_time_and_timeoff_sync_child_id = f"wcs_time_sync_to_quickbooks_time_sync_to_quickbooks_replicon_qbo_time_and_timeoff_sync_child_{instance}"
delete_approved_timesheet_from_log_id = f"wcs_time_sync_to_quickbooks_delete_approved_timesheet_from_log_{instance}"

# Token variable
wcs_time_sync_to_quickbooks_bearer_token_var = f"wcs_time_sync_to_quickbooks_bearer_token_variable_{instance}"

# Variable
lookup_log_timestamp_var = f"wcs_time_sync_to_quickbooks_lookup_log_timestamp_{instance}"
tenant_wide_log_var = f"wcs_time_sync_to_quickbooks_tenant_wide_log_{instance}"

# QBO connection
intuit_conn_id = 'qbo_WCSafmig_intuit'

# Email configuration
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
bcc_on_success = "{{ var.value.dagrun_internal_testing_email }}"
bcc_on_error = "{{ var.value.dagrun_internal_testing_email }}"

# Mapper
WCS_PAY_ITEM_REFERENCE_MAPPER = wcs_pay_item_reference_mapper