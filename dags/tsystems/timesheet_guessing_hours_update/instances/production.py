from tsystems.timesheet_guessing_hours_update.config import *
from tsystems.timesheet_guessing_hours_update.mapper.schedule_mapper_prod import schedule_mapper

instance = "production"
environment = 'production'

company_key = 'Tsystems'

replicon_conn_id = "tsystems_replicon_repliconint.timeimport"

tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f"tsystems_timesheet_guessing_hours_update_master_{instance}"
process_each_orgstructure = f"tsystems_timesheet_guessing_hours_update_process_each_orgstructure_child_{instance}"
process_users = f"tsystems_timesheet_guessing_hours_update_process_users_child_{instance}"
process_each_entry = f"tsystems_timesheet_guessing_hours_update_process_each_entry_child_{instance}"
process_log_generation = f"tsystems_timesheet_guessing_hours_update_process_log_generation_child_{instance}"

can_run_batch_task = f"tsystems_timesheet_guessing_hours_update_can_run_batch_task_{instance}_var"

SCHEDULE_MAPPER = schedule_mapper