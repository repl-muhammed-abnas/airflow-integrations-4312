# # pylint: disable=wildcard-import unused-wildcard-import
from macquariegroup.user_import.config import *
from macquariegroup.user_import.mappers.timesheet_approval_mapper import TIMESHEET_APPROVAL_MAPPER

instance = "uat"

timesheet_approval_mapper = TIMESHEET_APPROVAL_MAPPER

company_key = "macquarieproductiontrial01"
master_dag_interval = 30
dag_max_active_tasks = 32
execution_timeout_days = 14
child_dag_disableuser_max_active_runs = 16
child_dag_active_runs = 5

sftp_conn_id = "sftp_macquarie_22007"
replicon_conn_id = "macquarieproductiontrial01-replicon-tuser"

input_filepath = "/User Import/Sandbox/Processing"
archive_filepath = "/User Import/Sandbox/Archive"
log_filepath = "/User Import/Sandbox/Log"

user_import_reference_file_path = "/User Import/Sandbox/reference/user_import_reference_file.csv"
user_import_reference_file_archive_filepath = "/User Import/Sandbox/reference/archive/"

recovery_reconciliation_reference_filepath = "/Reconciliation import/Sandbox/reference/"
recovery_reconciliation_reference_archive_filepath = "/Reconciliation import/Sandbox/reference/archive/"

timezone = "Etc/UTC"
australia_holiday_calender = "Holidays for Australia"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

parallel_count = 10

can_run_batch_task_var_name = f"macquire_user_import_can_run_batch_task_var_{instance}"
