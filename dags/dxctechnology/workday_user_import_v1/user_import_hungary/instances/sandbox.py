from datetime import timedelta

from dxctechnology.workday_user_import_v1.user_import_hungary.config import *
from dxctechnology.workday_user_import_v1.user_import_hungary.mapper import (activities_mapper,
    timeoff_mapper, authentication_and_product, company_code_mapper, hun_general_mapper)

# Define master mapper if missing
from dxctechnology.workday_user_import_v1.user_import_hungary.mappers.master_mapper import MAPPER
MASTER_MAPPER = MAPPER

DAG_BATCH_COUNT

instance = "sandbox"
region = "us-east-2"
environment = "pre-production"

company_key = "dxcsandbox"

replicon_conn_id = "replicon_dxcsandbox_x.workday_5"
sftp_conn_id = "sftp_dxcsandbox_628172_Workday"

input_file_path = "/Test/Input/HUN"
archive_file_path = "Test/Archives"
log_file_path = "/Test/Logs"

UDFs = HUNGARY_UDF

schedule_interval = timedelta(seconds =30)

ACTIVITY_MAPPER = activities_mapper.ACTIVITY_MAPPER
GENERAL_MAPPER = hun_general_mapper.MAPPER_DATA
TIMEOFF_MAPPER = timeoff_mapper.TIMEOFF_MAPPER
PRODUCT = authentication_and_product.PRODUCT
AUTHS = authentication_and_product.AUTHENTICATION
COMPANY_CODE_MAPPER = company_code_mapper.COMPANY_CODES

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version:str = "" # _v1, _v2, etc. can be added here when creating a new version for the hungary
instance_version = f"_{instance}{version if version else ''}"

description_version_suffix = version.replace('_', '') if version else ''

# DAG IDs and description for hungary workflows
workday_user_import_hungary_main_dag = f"dxctechnology_workday_user_import_hungary_master{instance_version}"
workday_user_import_hungary_main_dag_description = f"DXC Workday User Import hungary - Process Data MASTER {description_version_suffix}"

workday_user_import_hungary_process_users_child_dag = f"dxctechnology_workday_user_import_hungary_process_users_child{instance_version}"
workday_user_import_hungary_process_users_child_dag_description = f"DXC Workday User Import hungary - Process Users Child DAG {description_version_suffix}"

workday_user_import_hungary_add_user_dag = f"dxctechnology_workday_user_import_hungary_add_user_child{instance_version}"
workday_user_import_hungary_add_user_dag_description = f"DXC Workday User Import hungary - Process Add User {description_version_suffix}"

workday_user_import_hungary_update_user_dag = f"dxctechnology_workday_user_import_hungary_update_user_child{instance_version}"
workday_user_import_hungary_update_user_dag_description = f"DXC Workday User Import hungary - Process Update User {description_version_suffix}"

workday_user_import_hungary_add_user_timeoff_assignment_dag = f"dxctechnology_workday_user_import_hungary_add_user_timeoff_assignment_child{instance_version}"
workday_user_import_hungary_add_user_timeoff_assignment_dag_description = f"DXC Workday User Import hungary - Process Add User Timeoff Assignment {description_version_suffix}"


workday_user_import_hungary_update_user_timeoff_assignment_dag = f"dxctechnology_workday_user_import_hungary_update_user_timeoff_assignment_child{instance_version}"
workday_user_import_hungary_update_user_timeoff_assignment_dag_description = f"DXC Workday User Import hungary - Process Update User Timeoff Assignment {description_version_suffix}"


workday_user_import_hungary_process_no_accrual_for_timeoff_dag = f"dxctechnology_workday_user_import_hungary_process_no_accrual_for_timeoff_child{instance_version}"
workday_user_import_hungary_process_no_accrual_for_timeoff_dag_description = f"DXC Workday User Import hungary - Process No Accrual for timeoff {description_version_suffix}"


workday_user_import_hungary_log_generation_dag = f"dxctechnology_workday_user_import_hungary_log_generation_child{instance_version}"
workday_user_import_hungary_log_generation_dag_description = f"DXC Workday User Import hungary - Process Log Generation {description_version_suffix}"

workday_user_import_hungary_user_rehire_timeoff_process_dag = f"dxctechnology_workday_user_import_hungary_user_rehire_timeoff_assignment_process_child{instance_version}"
workday_user_import_hungary_user_rehire_timeoff_process_dag_description = f"DXC Workday User Import hungary - Process Rehire User Timeoff {description_version_suffix}"

can_run_batch_task_var_name_hungary = "dxc_workday_can_run_batch_task_var_name_hungary_user_import"

process_time_off_accrual = f"dxctechnology_workday_user_import_hungary_user_timeoff_no_accrual_policy_update_child{instance_version}"
process_time_off_accrual_description = f"Dxctechnology Workday User Import hungary User Timeoff No Accrual Policy Update Child {description_version_suffix}"

process_log_generation_dagid_phl = f"dxctechnology_workday_user_import_hungary_process_log_generation{instance_version}"

workday_user_import_process_supervisor_assignment = f"dxctechnology_workday_user_import_hungary_process_supervisor_assignment_child{instance_version}"

workday_user_import_ia_zero_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_hungary_ia_zero_timeoff_assignment_child{instance_version}"
workday_user_import_ia_one_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_hungary_ia_one_timeoff_assignment_child{instance_version}"

# Cleanup child DAG ID for disabled users
delete_future_entries_child_dag_id = f"dxctechnology_workday_user_sync_delete_future_entries_child_{instance}_v2"

# Overrides: bump max_active_runs to 10 for this environment (base defaults in config.py are unchanged, so production is not affected)
max_active_run_add_user_hungary = 10
max_active_run_add_user_timeoff_assignemnt_hungary = 10
max_active_run_process_each_users_hungary = 10
max_active_run_process_ia_0_timeoff_assignment = 10
max_active_run_process_ia_1_timeoff_assignment = 10
max_active_run_process_timeoff_no_accrual = 10
max_active_run_rehire_user_timeoff_assignement_hungary = 10
max_active_run_update_user_hungary = 10
max_active_run_update_user_timeoff_assignment_hungary = 10
process_log_generation_max_active_runs = 10
