# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.user_import_v1.config import *
from mammoet.user_import_v1.mappers.timesheet_mapper import TIMESHEET_MAPPER
from mammoet.user_import_v1.mappers.timeoff_mapper_2 import TIMEOFF_MAPPER
from mammoet.user_import_v1.mappers.timezone_mapper import TIMEZONE_MAPPER
from mammoet.user_import_v1.mappers.activity_assignment_mapper import ACTIVITY_MAPPER
from mammoet.user_import_v1.mappers.location_code_mapper import LOCATION_CODE_MAPPER
from mammoet.user_import_v1.mappers.work_week_mapper import WORK_WEEK_MAPPER
from mammoet.user_import_v1.mappers.timesheet_period_mapper import TIMESHEET_PERIOD_MAPPER

instance = "trial"

environment = "pre-production"

company_key = "mammoettrial01trial01"

replicon_conn_id = "mammoettrial01trial01_replicon_admin"
sftp_conn_id = "sftp_useast2"
log_filepath = "“/User Import/Trial01Trial01/Log"

mammoet_user_import_bearer_token_variable = "mammoet_user_import_bearer_token_variable_trial"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

user_import_master_dag_id = f"mammoet_user_import_master_webhook_{instance}"
user_import_process_payload_child_dag_id = f"mammoet_user_import_process_payload_child_{instance}_v1"
user_import_process_groups_child_dag_id = f"mammoet_user_import_process_groups_child_{instance}_v1"
user_import_add_location_child_dag_id = f"mammoet_user_import_add_location_child_{instance}_v1"
user_import_process_users_child_dag_id =  f"mammoet_user_import_process_users_child_{instance}_v1"
user_import_add_users_child_dag_id =  f"mammoet_user_import_add_users_child_{instance}_v1"
user_import_indirect_employee_add_users_child_dag_id =  f"mammoet_user_import_indirect_employee_add_users_child_{instance}_v1"
user_import_update_users_child_dag_id =  f"mammoet_user_import_update_users_child_{instance}_v1"
user_import_process_supervisor_assignment_dag_id = f"mammoet_user_import_process_supervisor_assignment_child_{instance}_v1"
process_log_generation_dagid = f"mammoet_user_import_process_log_generation_child_{instance}_v1"

disable_user_child_dagid = f"mammoet_user_import_disable_user_child_{instance}_v1"
disable_user_main_dagid = f"mammoet_user_import_disable_user_master_{instance}_v1"

TIMESHEET_MAPPER_TO_USE  = TIMESHEET_MAPPER
TIMEOFF_MAPPER_TO_USE = TIMEOFF_MAPPER
TIMEZONE_MAPPER_TO_USE = TIMEZONE_MAPPER
ACTIVITY_MAPPER_TO_USE = ACTIVITY_MAPPER
LOCATION_CODE_MAPPER_TO_USE = LOCATION_CODE_MAPPER
WORK_WEEK_MAPPER_TO_USE = WORK_WEEK_MAPPER
TIMESHEET_PERIOD_MAPPER_TO_USE = TIMESHEET_PERIOD_MAPPER

can_run_batch_task_var_name = f"mammoet_user_import_can_run_batch_task_var_{instance}"

disabled = True
