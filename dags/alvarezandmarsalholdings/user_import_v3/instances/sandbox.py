from alvarezandmarsalholdings.user_import_v3.config import *
from alvarezandmarsalholdings.user_import_v3.mappers.timesheet_template_mapper_v1 import timesheet_template_mapper
from alvarezandmarsalholdings.user_import_v3.mappers.timezone_mapper_sb import timezone_mapper
from alvarezandmarsalholdings.user_import_v3.mappers.holiday_calendar_mapper_sb import holiday_calendar_mapper
from alvarezandmarsalholdings.user_import_v3.mappers.timeoff_type_mapper_sb import time_off_mapper
from alvarezandmarsalholdings.user_import_v3.mappers.general_mapper import general_mapper

instance = 'sandbox'
environment = 'pre-production'
company_key = 'alvarezandmarsalsb'
replicon_conn_id = 'alvarezandmarsalsb_replicon_repliconint.userimport'
sftp_conn_id = 'sftp_alvarezandmarsalsb_621229'


tenant_email = 'ITERP@alvarezandmarsal.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = "/SB/User Import from Workday/Logs"

user_import_master_dagid = f'alvarezandmarsalholdings_user_import_master_{instance}_v3'
process_users_dagid = f'alvarezandmarsalholdings_user_import_process_users_child_{instance}_v3'
process_new_users_dagid = f'alvarezandmarsalholdings_user_import_process_new_user_child_{instance}_v3'
process_update_users_dagid = f'alvarezandmarsalholdings_user_import_process_update_user_child_{instance}_v3'
assign_supervisor_dag_id = f'alvarezandmarsalholdings_user_import_assign_supervisor_child_{instance}_v3'
schedule_add_dag_id = f'alvarezandmarsalholdings_user_import_schedule_add_child_{instance}_v3'
process_log_generation_dag_id = f'alvarezandmarsalholdings_user_import_process_logs_child_{instance}_v3'
update_oef_dropdown_dag_id = f'alvarezandmarsalholdings_user_import_update_oef_dropdown_child_{instance}_v3'
process_cost_centers_dagid = f'alvarezandmarsalholdings_user_import_process_cost_centers_child_{instance}_v3'

TIMESHEET_TEMPLATE_MAPPER = timesheet_template_mapper
TIMEZONE_MAPPER = timezone_mapper
HOLIDAY_CALENDAR_MAPPER = holiday_calendar_mapper
TIMEOFF_MAPPER = time_off_mapper
GENERAL_MAPPER = general_mapper

can_run_batch_task_var_name = f'alvarezandmarsalholdings_user_import_process_users_batch_task_var_{instance}_v3'