from alvarezandmarsalholdings.user_import_v1.config import *
from alvarezandmarsalholdings.user_import_v1.mappers.timesheet_template_mapper import timesheet_template_mapper
from alvarezandmarsalholdings.user_import_v1.mappers.timezone_mapper import timezone_mapper
from alvarezandmarsalholdings.user_import_v1.mappers.holiday_calendar_mapper import holiday_calendar_mapper
from alvarezandmarsalholdings.user_import_v1.mappers.timeoff_type_mapper import time_off_mapper
from alvarezandmarsalholdings.user_import_v1.mappers.general_mapper import general_mapper


instance = 'dev'
environment = 'pre-production'
company_key = 'alvarezandmarsalholdingsdev'
replicon_conn_id = 'alvarezandmarsalholdingsdev_replicon_radmin1'
sftp_conn_id = 'sftp_alvarezandmarsalholdingsdev_621229'


tenant_email = 'ITERP@alvarezandmarsal.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = "/Dev/User Import from Workday/Logs"

user_import_master_dagid = f'alvarezandmarsalholdings_user_import_master_{instance}_v1'
process_users_dagid = f'alvarezandmarsalholdings_user_import_process_users_child_{instance}_v1'
process_new_users_dagid = f'alvarezandmarsalholdings_user_import_process_new_user_child_{instance}_v1'
process_update_users_dagid = f'alvarezandmarsalholdings_user_import_process_update_user_child_{instance}_v1'
assign_supervisor_dag_id = f'alvarezandmarsalholdings_user_import_assign_supervisor_child_{instance}_v1'
schedule_add_dag_id = f'alvarezandmarsalholdings_user_import_schedule_add_child_{instance}_v1'
process_log_generation_dag_id = f'alvarezandmarsalholdings_user_import_process_logs_child_{instance}_v1'
update_oef_dropdown_dag_id = f'alvarezandmarsalholdings_user_import_update_oef_dropdown_child_{instance}_v1'
process_cost_centers_dagid = f'alvarezandmarsalholdings_user_import_process_cost_centers_child_{instance}_v1'


TIMESHEET_TEMPLATE_MAPPER = timesheet_template_mapper
TIMEZONE_MAPPER = timezone_mapper
HOLIDAY_CALENDAR_MAPPER = holiday_calendar_mapper
TIMEOFF_MAPPER = time_off_mapper
GENERAL_MAPPER = general_mapper

can_run_batch_task_var_name = f'alvarezandmarsalholdings_user_import_process_users_batch_task_var_{instance}_v1'

disabled=True
