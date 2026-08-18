# pylint: disable=wildcard-import unused-wildcard-import
from macquariegroup.recovery_reconciliation.config import *

instance = "trial"

company_key = "macquarieproductiontrial01"
master_dag_interval = 30


# Clients SFTP
sftp_conn_id = "Airflow_migration_SFTP_eucentral"
replicon_conn_id = "macquarieproductiontrial01-replicon-tuser"

# Trial will be changed to Sandbox for UAT
input_filepath = "/macquarie/Reconciliation import/Trial/Input"
archive_filepath = "/macquarie/Reconciliation import/Trial/Archive"
log_filepath = "/macquarie/Reconciliation import/Trial/log"
recovery_reconciliation_reference_filepath = "/macquarie/Reconciliation import/Trial/reference/"
recovery_reconciliation_reference_archive_filepath = "/macquarie/Reconciliation import/Trial/reference/archive/"
alert_notification_log_filepath = "/macquarie/Reconciliation import/Trial/custom_notification/Logs"

user_import_input_filepath = "/macquarie/User import/Trial/Input"
user_import_processing_filepath = "/macquarie/User import/Trial/Processing"
user_import_archive_filepath = "/macquarie/User import/Trial/Archive"

reconn_reference_filepath = ""
timezone = "Etc/UTC"
user_base_report = "***Reconciliation User Base Report Dev"

custom_notification_base_report = "*** Custom Notification Base report QA"
australia_holiday_calender = "Holidays for Australia QA"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
