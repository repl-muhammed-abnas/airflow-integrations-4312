# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.time_off_booking_import_v2.config import *

instance = "uat"

company_key = "mammoettrial01"
replicon_conn_id = "mammoettrial01_replicon_admin"
sftp_conn_id = "sftp_mammoet_uat"

log_filepath = "/Time Off Import/Trial01/Log"

tenant_email = 'repliconnotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'


process_timeoff_import_payload_dagid = f"mammoet_timeoff_booking_import_process_payload_child_{instance}_v2"
process_distinct_employees_dagid = f"mammoet_timeoff_booking_import_process_distinct_employees_child_{instance}_v2"
process_each_time_off_entry_dagid = f"mammoet_timeoff_booking_import_process_each_timeoff_entry_child_{instance}_v2"
process_log_generation_dagid = f"mammoet_timeoff_booking_import_process_log_generation_child_{instance}_v2"

can_run_batch_task_var_name = f'mammoet_timeoff_booking_import_run_batch_task_{instance}'
allow_same_day_timeoff_start = True