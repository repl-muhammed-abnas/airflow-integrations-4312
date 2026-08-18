# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.optional_holidays_auto_population_india_new_users_v1.config import *

instance = 'sit'
environment = 'pre-production'

company_key = 'capgeminisit'

replicon_conn_id = 'capgeminisit_replicon_optional_holiday_admin'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiSIT'

log_filepath = "/Internal/Optional_Holiday_Booking/Logs"
s3_log_filepath = "CapgeminiSIT/Internal/Optional_Holiday_Booking/Logs"

states_optional_holiday_calendars = f"capgemini_states_optional_holiday_calendars_mapper_{instance}"

max_active_runs = 1
max_active_runs_new_users = 10
max_active_holiday_booking_child = 10
dag_max_active_tasks = 10000
parallel_dags_count = 10

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'capgemini_optional_holiday_booking_new_users_can_run_batch_task_{instance}'
can_run_batch_task_booking_child_var_name = f'capgemini_optional_holiday_booking_child_new_users_can_run_batch_task_{instance}'
excepted_timeoff_types_mapper = f'capgemini_optional_holiday_booking_excepted_timeoff_types_{instance}'

tenant_wide_log = f'{company_key}_new_users_log'
# Do not edit version in old_tenant_wide_log
old_tenant_wide_log = f'capgemini_auto_population_of_optional_holidays_new_users_tenant_wide_log_{instance}_v0'

log_file_prefix = "Sit"

tenant_log = f"artifact:CapgeminiSIT:log:{tenant_wide_log}"
old_tenant_log = f"artifact:CapgeminiSIT:log:{old_tenant_wide_log}"
tenant_wide_log_list = [old_tenant_log,
                        f"{tenant_log}_0",
                        f"{tenant_log}_1",
                        f"{tenant_log}_2",
                        f"{tenant_log}_3",
                        f"{tenant_log}_4"
                    ]

master_dagid = f'capgemini_auto_population_of_optional_holidays_india_new_users_master_{instance}_v1'
process_new_users_dagid = f'capgemini_auto_population_of_optional_holidays_india_process_new_users_{instance}_v1'
booking_child_dagid = f'capgemini_auto_population_of_optional_holidays_india_book_optional_holiday_child_new_user_{instance}_v1'
