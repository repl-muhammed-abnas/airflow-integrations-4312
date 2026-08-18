# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.time_off_booking_import_v2.config import *

instance = "trial"

company_key = "mammoettrial01trial01"
replicon_conn_id = "mammoettrial01trial01_replicon_admin"
sftp_conn_id = "sftp_useast2"

log_filepath = "Mammoet/Time Off Import/Trial01Trial01/Log"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'


process_timeoff_import_payload_dagid = f"mammoet_timeoff_booking_import_process_payload_child_{instance}_v2"
process_distinct_employees_dagid = f"mammoet_timeoff_booking_import_process_distinct_employees_child_{instance}_v2"
process_each_time_off_entry_dagid = f"mammoet_timeoff_booking_import_process_each_timeoff_entry_child_{instance}_v2"
process_log_generation_dagid = f"mammoet_timeoff_booking_import_process_log_generation_child_{instance}_v2"

# webhook master added under webhook endpoints folder
webhook_master_dagid = f"mammoet_timeoff_booking_import_webhook_master_{instance}"
mammoet_timeoff_booking_import_bearer_token_var = f"mammoet_timeoff_booking_import_bearer_token_variable_{instance}"

can_run_batch_task_var_name = f'mammoet_timeoff_booking_import_run_batch_task_{instance}'

disabled=True
allow_same_day_timeoff_start = True
