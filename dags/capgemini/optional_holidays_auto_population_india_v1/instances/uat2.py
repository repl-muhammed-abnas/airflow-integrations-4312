# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.optional_holidays_auto_population_india_v1.config import *

instance = 'uat2'
environment = 'pre-production'

company_key = 'capgeminiuat2'

replicon_conn_id = 'capgeminiuat2_replicon_optional_holiday_admin'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'

log_filepath = "/Internal/Optional_Holiday_BookingUAT2/Logs"
s3_log_filepath = "CapgeminiUAT/Internal/Optional_Holiday_BookingUAT2/Logs"

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
excepted_timeoff_types_mapper = f'capgemini_optional_holiday_booking_excepted_timeoff_types_{instance}'

log_file_prefix = "Uat2"

booking_child_dagid = f'capgemini_auto_population_of_optional_holidays_book_optional_holiday_child_{instance}_v1'
booking_child_dagid_1 = f'capgemini_auto_population_of_optional_holidays_book_optional_holiday_child_batch_1_{instance}_v1'
booking_child_dagid_2 = f'capgemini_auto_population_of_optional_holidays_book_optional_holiday_child_batch_2_{instance}_v1'
booking_child_dagid_3 = f'capgemini_auto_population_of_optional_holidays_book_optional_holiday_child_batch_3_{instance}_v1'
booking_child_dagid_4 = f'capgemini_auto_population_of_optional_holidays_book_optional_holiday_child_batch_4_{instance}_v1'
process_esisting_users_dagid = f'capgemini_auto_population_of_optional_holidays_process_existing_users_optional_holidays_child_{instance}_v1'
existing_user_master_dagid = f'capgemini_auto_population_of_optional_holidays_india_existing_users_master_{instance}_v1'
timeoff_status_update_master = f'capgemini_auto_population_of_optional_holidays_india_timeoff_status_change_master_{instance}_v1'
trigger_booking_batch_childs_dagid = f'capgemini_auto_population_of_optional_holidays_book_optional_holiday_trigger_booking_batch_child_{instance}_v1'
approve_timeoff_child_dagid = f'capgemini_auto_population_of_optional_holidays_approve_timeoff_child_{instance}_v1'
delete_timeoff_child_dagid = f'capgemini_auto_population_of_optional_holidays_delete_timeoff_child_{instance}_v1'

disabled=True
