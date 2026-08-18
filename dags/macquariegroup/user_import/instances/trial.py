# pylint: disable=wildcard-import unused-wildcard-import
from macquariegroup.user_import.config import *
from macquariegroup.user_import.mappers.timesheet_approval_mapper import TIMESHEET_APPROVAL_MAPPER

instance = "trial"

timesheet_approval_mapper = TIMESHEET_APPROVAL_MAPPER

company_key = "macquarieproductiontrial01"
master_dag_interval = 30
dag_max_active_tasks = 32
execution_timeout_days = 14
child_dag_disableuser_max_active_runs = 16
child_dag_active_runs = 5

# Clients SFTP
sftp_conn_id = "Airflow_migration_SFTP_eucentral"
replicon_conn_id = "macquarieproductiontrial01-replicon-tuser"

# Trial will be changed to Sandbox for UAT
input_filepath = "/macquarie/User import/Trial/Processing"
archive_filepath = "/macquarie/User import/Trial/Archive"
log_filepath = "/macquarie/User import/Trial/log"

user_import_reference_file_path = "/macquarie/User import/Trial/reference/user_import_reference_file.csv"
user_import_reference_file_archive_filepath = "/macquarie/User import/Trial/reference/archive/"

recovery_reconciliation_reference_filepath = "/macquarie/Reconciliation import/Trial/reference/"
recovery_reconciliation_reference_archive_filepath = "/macquarie/Reconciliation import/Trial/reference/archive/"

timezone = "Etc/UTC"
australia_holiday_calender = "Holidays for Australia QA"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

parallel_count = 10

user_import_base_report_name = "***User Import Base report QA"

can_run_batch_task_var_name = f"macquire_user_import_can_run_batch_task_var_{instance}"
disabled = True
