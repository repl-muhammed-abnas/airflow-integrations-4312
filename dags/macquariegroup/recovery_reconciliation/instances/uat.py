# pylint: disable=wildcard-import unused-wildcard-import
from macquariegroup.recovery_reconciliation.config import *

instance = "uat"

company_key = "macquarieproductiontrial01"
master_dag_interval = 30


sftp_conn_id = "sftp_macquarie_22007"
replicon_conn_id = "macquarieproductiontrial01-replicon-tuser"

input_filepath = "/Reconciliation import/Sandbox/Input"
archive_filepath = "/Reconciliation import/Sandbox/Archive"
log_filepath = "/Reconciliation import/Sandbox/Log"
recovery_reconciliation_reference_filepath = "/Reconciliation import/Sandbox/reference/"
recovery_reconciliation_reference_archive_filepath = "/Reconciliation import/Sandbox/reference/archive/"
alert_notification_log_filepath = "/Reconciliation import/Sandbox/custom_notification/Logs"

user_import_input_filepath = "/User Import/Sandbox/Input"
user_import_processing_filepath = "/User Import/Sandbox/Processing"
user_import_archive_filepath = "/User Import/Sandbox/Archive"

reconn_reference_filepath = ""
timezone = "Etc/UTC"
user_base_report = "***Reconciliation User Base Report"

custom_notification_base_report = "*** Custom Notification Base report"
australia_holiday_calender = "Holidays for Australia"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
