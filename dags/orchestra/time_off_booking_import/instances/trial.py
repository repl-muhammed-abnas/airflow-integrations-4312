# pylint: disable=wildcard-import unused-wildcard-import
from orchestra.time_off_booking_import.config import *

instance = "trial"

company_key = "orchestragroupllctrial01"

replicon_conn_id = "orchestragroupllctrial01_replicon_admin"
sftp_conn_id = "rsftp-useast_for_testing"

input_filepath = "orchestra/Time Off Import/Input"
log_filepath = "orchestra/Time Off Import/Log"
archive_filepath = "orchestra/Time Off Import/Archive"
sftp_reference_filepath = "orchestra/Time Off Import/Reference"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'


process_timeoff_import_master_dagid = f"orchestra_timeoff_booking_import_master_{instance}"
process_distinct_employees_dagid = f"orchestra_timeoff_booking_import_process_each_user_child_{instance}"
process_each_timeoff_dagid = f"orchestra_timeoff_booking_import_process_each_timeoff_child_{instance}"

can_run_batch_task_var_name = f'orchestra_timeoff_booking_import_run_batch_task_{instance}'
