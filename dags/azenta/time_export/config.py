"""
Configuration for Azenta Polaris → Oracle PPM Time Export Integration (FI017)
Exports approved worked-time entries from Replicon Polaris to Oracle Fusion Cloud PPM project costs.
"""

region = "us-east-1"
environment = "pre-production"

# DAG execution
max_active_run = 1
execution_timeout_days = 1

# Scheduling: spec §6.0 — every 30 minutes. send_empty_export_email is left as-is (will fire on
# every no-data run at this cadence); revisit if that turns out to be too noisy.
schedule_interval = "*/30 * * * *"
timezone = "America/New_York"

# Replicon report template name (required per RAIL integration standards for report pattern)
time_export_file_format = 'Time Data Export - Oracle'

# Oracle REST API version
oracle_api_version = "11.13.18.05"

# Oracle endpoint paths (host configured in Airflow connection; paths are relative)
oracle_hcm_public_workers_path = f"/hcmRestApi/resources/{oracle_api_version}/publicWorkers"

# Oracle payload constants — hardcoded per the tech-spec mapping in the client-validated Postman
# collection ("Oracle PPM SOAP - ProjectTimecardService (Unprocessed Labor Transaction V3) Copy 3")
oracle_soap_source_name = "Labor Hours"
oracle_soap_document_name = "Replicon Labor Hours"
oracle_soap_document_entry_name = "Replicon Labor Hours"
oracle_soap_expenditure_type_name = "Labor Hours"
oracle_soap_unit_of_measure = "Hours"
# 'Y' (unmatched) for every negative-Quantity row (Replicon hours-reduction corrections) — see
# build_receive_timecard_row's comment for why 'N' (matched) is not safe here.
oracle_soap_unmatched_negative_txn_flag = "Y"
# Prefix for OriginalTransactionReference to ensure global uniqueness in Oracle
oracle_transaction_ref_prefix = "REPLICON-"

# Oracle SOAP endpoint for bulk timecard-transaction submission (same Oracle env/connection as
# REST — reuses oracle_http_conn_id in instances/*.py; WS-Security credentials are also sourced
# from that same connection's Login/Password, not a separate connection)
oracle_soap_project_txn_path = "/fscmService/ProjectTimecardService"
oracle_soap_action_project_txn = (
    "http://xmlns.oracle.com/apps/projects/costing/transactions/"
    "transactionServiceV3/receiveTimecardTransaction"
)

# Inferred by analogy to oracle_soap_action_project_txn's validated pattern (Oracle's WSA response
# Action header differs — it includes a "/ProjectTimecardService/" segment and "Response" suffix,
# which is normal for SOAP responses vs. requests). Confirm against the WSDL/Postman collection
# before go-live if a validate call ever returns an unexpected SOAPAction fault.
oracle_soap_action_validate_txn = (
    "http://xmlns.oracle.com/apps/projects/costing/transactions/"
    "transactionServiceV3/validateTimecardTransaction"
)

# Airflow Variable kill-switch base names (instance suffix appended in DAG)
can_run_batch_task_var_name = "azenta_time_export_can_run_batch_task"
can_post_to_oracle_var_name = "azenta_time_export_can_post_to_oracle"

# Accounting cutoff — monthly close rule: entries from month M eligible if approved by day 1
# of month M+1 at or before this hour (Eastern). Entries approved after cutoff: manual handling.
accounting_cutoff_hour = 17  # 5:00 PM Eastern

# Replicon's TimeDataExportService allows only one of these two approval filters per export —
# "timesheet" filters on timesheet-level approval status, "time_entry" filters per-time-entry
# approval status instead. Change this if the client's approval workflow changes; no code change
# needed (see request_payload.APPROVAL_FILTER_URI_BY_MODE for the allowed values).
time_export_approval_filter_mode = "timesheet"

# Project status post-load filter — only entries for projects in these statuses are sent to Oracle
eligible_project_statuses = ["Execution", "In Progress"]

# Per-record CSV report (Login/Entry Date/Hours/Status/Message) attached as a download link to the
# success/validation-failure/posting-failure emails, and archived to SFTP — see report_sftp_conn_id/
# report_sftp_remote_dir in instances/*.py. 7 days matches this repo's presigned-link convention.
report_download_link_expires_in_seconds = 7 * 24 * 60 * 60
