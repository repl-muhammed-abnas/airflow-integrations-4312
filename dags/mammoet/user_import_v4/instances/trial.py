# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.user_import_v4.config import *
from mammoet.user_import_v4.mappers.timesheet.timesheet_mapper_v4 import TIMESHEET_MAPPER
from mammoet.user_import_v4.mappers.timeoff.timeoff_mapper_6 import TIMEOFF_MAPPER_UAT
from mammoet.user_import_v4.mappers.timezone.timezone_mapper import TIMEZONE_MAPPER
from mammoet.user_import_v4.mappers.activity.activity_assignment_mapper_v6 import ACTIVITY_MAPPER
from mammoet.user_import_v4.mappers.location_code.location_code_mapper_v2 import LOCATION_CODE_MAPPER
from mammoet.user_import_v4.mappers.work_week.work_week_mapper_v2 import WORK_WEEK_MAPPER
from mammoet.user_import_v4.mappers.timesheet_period.timesheet_period_mapper_v2 import TIMESHEET_PERIOD_MAPPER

instance = "trial"

environment = "pre-production"

company_key = "mammoettrial02"

replicon_conn_id = "mammoettrial02_replicon_admin"
sftp_conn_id = "sftp_useast2"
log_filepath = "/Mammoet/User Import/Trial01Trial01/Log"

mammoet_user_import_bearer_token_variable = "mammoet_user_import_bearer_token_variable_trial"

tenant_email ='{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

user_import_master_dag_id = f"mammoet_user_import_master_webhook_{instance}"
user_import_process_payload_child_dag_id = f"mammoet_user_import_process_payload_child_{instance}_v4"
user_import_process_groups_child_dag_id = f"mammoet_user_import_process_groups_child_{instance}_v4"
user_import_add_location_child_dag_id = f"mammoet_user_import_add_location_child_{instance}_v4"
user_import_process_multiple_users_child_dag_id = f"mammoet_user_import_process_multiple_users_child_{instance}_v4"
user_import_process_users_child_dag_id =  f"mammoet_user_import_process_users_child_{instance}_v4"
user_import_add_users_child_dag_id =  f"mammoet_user_import_add_users_child_{instance}_v4"
user_import_indirect_employee_add_users_child_dag_id =  f"mammoet_user_import_indirect_employee_add_users_child_{instance}_v4"
user_import_update_users_child_dag_id =  f"mammoet_user_import_update_users_child_{instance}_v4"
user_import_process_supervisor_assignment_dag_id = f"mammoet_user_import_process_supervisor_assignment_child_{instance}_v4"
process_log_generation_dagid = f"mammoet_user_import_process_log_generation_child_{instance}_v4"

TIMESHEET_MAPPER_TO_USE  = TIMESHEET_MAPPER
TIMEOFF_MAPPER_TO_USE = TIMEOFF_MAPPER_UAT
TIMEZONE_MAPPER_TO_USE = TIMEZONE_MAPPER
ACTIVITY_MAPPER_TO_USE = ACTIVITY_MAPPER
LOCATION_CODE_MAPPER_TO_USE = LOCATION_CODE_MAPPER
WORK_WEEK_MAPPER_TO_USE = WORK_WEEK_MAPPER
TIMESHEET_PERIOD_MAPPER_TO_USE = TIMESHEET_PERIOD_MAPPER

can_run_batch_task_var_name = f"mammoet_user_import_can_run_batch_task_var_{instance}"
