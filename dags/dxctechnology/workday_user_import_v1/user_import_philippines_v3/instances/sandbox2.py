from datetime import timedelta
from dxctechnology.workday_user_import_v1.user_import_philippines_v3.config import *
from dxctechnology.workday_user_import_v1.user_import_philippines_v3.mapper import (activities_mapper,
                holiday_calendar, phl_general_mapper, schedules_mapper, timeoff_mapper, authentication_and_product, company_code_mapper)

# Define master mapper if missing
from dxctechnology.workday_user_import_v1.user_import_philippines_v3.mappers.master_mapper import MAPPER
MASTER_MAPPER = MAPPER

DAG_BATCH_COUNT

instance = "sandbox2"
region = "us-east-2"
environment = "pre-production"

company_key = "dxcsandbox2"

replicon_conn_id = "replicon_dxcsandbox2_x.workday_1"
sftp_connection_id = "sftp_dxcsandbox2_628172_Workday"

input_file_path = "/Test/Input/PHL"
archive_file_path = "/Test/Archives"
log_file_path = "/Test/Logs"

UDFs = PHILIPPINES_UDF

schedule_interval = timedelta(seconds =30)

ACTIVITY_MAPPER = activities_mapper.ACTIVITY_MAPPER
HOLIDAY_CALENDAR = holiday_calendar.HOLIDAY_CALENDAR
GENERAL_MAPPER = phl_general_mapper.PHL_GENERAL_MAPPER
SCHEDULES_MAPPER = schedules_mapper.SCHEDULES_MAPPER
TIMEOFF_MAPPER = timeoff_mapper.TIMEOFF_MAPPER
PRODUCT = authentication_and_product.PRODUCT
AUTHS = authentication_and_product.AUTHENTICATION
COMPANY_CODE_MAPPER = company_code_mapper.COMPANY_CODES

# update with Deltek email IDs
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_emails = '{{ var.value.dagrun_internal_testing_email }}'


version: str = "_v3"  # _v2, _v2, etc. can be added here when creating a new version for the philippines
instance_version = f"_{instance}{version if version else ''}_batch"

description_version_suffix = version.replace('_', '') if version else ''

# DAG IDs and description for Philippines workflows
workday_user_import_process_philippines_data_child_dag = f"dxctechnology_workday_user_import_process_philippines_data_master{instance_version.replace('_batch', '')}"
workday_user_import_process_philippines_data_child_dag_description = f"DXC Workday User Import Philippines - Process Data Child DAG {description_version_suffix}"

workday_user_import_philippines_process_users_child_dag = f"dxctechnology_workday_user_import_philippines_process_users_child{instance_version}"
workday_user_import_philippines_process_users_child_dag_description = f"DXC Workday User Import Philippines - Process Users Child DAG {description_version_suffix}"

workday_user_import_philippines_add_user_dag = f"dxctechnology_workday_user_import_philippines_add_user{instance_version}"
workday_user_import_philippines_add_user_dag_description = f"DXC Workday User Import Philippines - Process Add User {description_version_suffix}"

workday_user_import_philippines_update_user_dag = f"dxctechnology_workday_user_import_philippines_update_user{instance_version}"
workday_user_import_philippines_update_user_dag_description = f"DXC Workday User Import Philippines - Process Update User {description_version_suffix}"

workday_user_import_philippines_add_user_timeoff_assignment_dag = f"dxctechnology_workday_user_import_philippines_add_user_timeoff_assignment{instance_version}"
workday_user_import_philippines_add_user_timeoff_assignment_dag_description = f"DXC Workday User Import Philippines - Process Add User Timeoff Assignment {description_version_suffix}"


workday_user_import_philippines_update_user_timeoff_assignment_dag = f"dxctechnology_workday_user_import_philippines_update_user_timeoff_assignment{instance_version}"
workday_user_import_philippines_update_user_timeoff_assignment_dag_description = f"DXC Workday User Import Philippines - Process Update User Timeoff Assignment {description_version_suffix}"


workday_user_import_philippines_process_no_accrual_for_timeoff_dag = f"dxctechnology_workday_user_import_philippines_process_no_accrual_for_timeoff{instance_version}"
workday_user_import_philippines_process_no_accrual_for_timeoff_dag_description = f"DXC Workday User Import Philippines - Process No Accrual for timeoff {description_version_suffix}"


workday_user_import_philippines_log_generation_dag = f"dxctechnology_workday_user_import_philippines_log_generation{instance_version}"
workday_user_import_philippines_log_generation_dag_description = f"DXC Workday User Import Philippines - Process Log Generation {description_version_suffix}"

workday_user_import_philippines_user_rehire_timeoff_process_dag = f"dxctechnology_workday_user_import_philippines_user_rehire_timeoff_assignment_process_child{instance_version}"
workday_user_import_philippines_user_rehire_timeoff_process_dag_description = f"DXC Workday User Import Philippines - Process Rehire User Timeoff {description_version_suffix}"


## philippines
max_active_run_process_each_users_philippines = 10
max_active_run_add_user_philippines = 10
max_active_run_add_user_timeoff_assignemnt_philippines = 10
max_active_run_update_user_timeoff_assignment_philippines = 10
max_active_run_update_user_philippines = 10
max_active_run_rehire_user_timeoff_assignement_philippines = 10
max_active_run_process_timeoff_no_accrual = 10
process_users_parallel_count = 5

can_run_batch_task_var_name_philippines = "dxc_workday_can_run_batch_task_var_name_philippines_user_import"

process_time_off_accrual = f"dxctechnology_workday_user_import_philippines_user_timeoff_no_accrual_policy_update_child{instance_version}"
process_time_off_accrual_description = f"Dxctechnology Workday User Import Philippines User Timeoff No Accrual Policy Update Child {description_version_suffix}"

process_log_generation_dagid_phl = f"dxctechnology_workday_user_import_philippines_process_log_generation{instance_version}"
process_log_generation_max_active_runs = 10

workday_user_import_process_supervisor_assignment = f"dxctechnology_workday_user_import_philippines_process_supervisor_assignment_child{instance_version}"

ia_version = "v3"
workday_user_import_ia_zero_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_philippines_ia_zero_timeoff_assignment_child_{instance}_{ia_version}"
workday_user_import_ia_one_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_philippines_ia_one_timeoff_assignment_child_{instance}_{ia_version}"

delete_future_entries_child_version = "v2"
# Cleanup child DAG ID for disabled users
delete_future_entries_child_dag_id = f"dxctechnology_workday_user_sync_delete_future_entries_child_{instance}_{delete_future_entries_child_version}"
