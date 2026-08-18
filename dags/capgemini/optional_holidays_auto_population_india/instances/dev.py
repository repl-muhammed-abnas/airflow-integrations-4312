# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.optional_holidays_auto_population_india.config import *

instance = 'dev'
environment = 'pre-production'

company_key = 'capgeminidev'

replicon_conn_id = 'capgeminidev_replicon_optional_holiday_admin'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiDev'

log_filepath = "/Internal/Optional_Holiday_Booking/Logs"
s3_log_filepath = "CapgeminiDev/Internal/Optional_Holiday_Booking/Logs"

states_optional_holiday_calendars = f"capgemini_states_optional_holiday_calendars_mapper_{instance}"

max_active_runs = 1
max_active_runs_new_users = 10
max_active_process_holidays_child = 5
max_active_holiday_booking_child = 10
dag_max_active_tasks = 10000
max_active_trigger_booking_child = 5

trigger_child_batch_parallel_dagrun_count = 50
trigger_booking_childs_batch_size = 5000
MAX_BATCH_ALLOWED = 5

max_active_approve_timeoff_child = 20
max_active_delete_timeoff_child = 20
approve_parallel_count = 50
delete_parallel_count = 10

lookup_log_timestamp_var = f'capgemini_optional_holiday_booking_lookup_log_timestamp_{instance}'
log_generation_dag_interval = '0 1 * * *'
# Need to be updated based on log_generation_dag_interval
lookup_log_timestamp_hours = 24

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'capgemini_optional_holiday_booking_can_run_batch_task_{instance}'
can_run_batch_task_booking_child_var_name = f'capgemini_optional_holiday_booking_child_can_run_batch_task_{instance}'
webhook_shared_secret = f'capgemini_optional_holiday_booking_new_user_webhook_secret_{instance}'
excepted_timeoff_types_mapper = f'capgemini_optional_holiday_booking_excepted_timeoff_types_{instance}'

log_file_prefix = "Dev"
disabled = True
