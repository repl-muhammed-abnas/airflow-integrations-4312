# UK&I CSC Trial Instance Configuration
from dxctechnology.workday_user_import_v1.user_import_uki_csc.config import * 
from dxctechnology.workday_user_import_v1.user_import_uki_csc.mapper.authentication_and_product import PRODUCT
from dxctechnology.workday_user_import_v1.user_import_uki_csc.mapper.company_code_mapper import COMPANY_CODE_MAPPER as COMPANY_CODE_MAPPING
instance = "trial"


PRODUCT = PRODUCT
company_key = "dxctrial01"

# DAG IDs
workday_user_import_process_uki_csc_data_child_dag = "dxctechnology_workday_user_import_master_uki_csc_trial"
workday_user_import_process_uki_csc_user_records_child_dag = "dxctechnology_workday_user_import_uki_csc_process_users_trial"
workday_user_import_uki_csc_add_user_dag = "dxctechnology_workday_user_import_uki_csc_add_user_trial"
workday_user_import_uki_csc_update_user_dag = "dxctechnology_workday_user_import_uki_csc_update_user_trial"
workday_user_import_uki_csc_add_user_timeoff_assignment_dag = "dxctechnology_workday_user_import_uki_csc_add_user_timeoff_assignment_trial"
workday_user_import_uki_csc_user_rehire_timeoff_process_dag = "dxctechnology_workday_user_import_uki_csc_user_rehire_timeoff_trial"
workday_user_import_uki_csc_update_user_timeoff_assignment_dag = "dxctechnology_workday_user_import_uki_csc_update_user_timeoff_assignment_trial"
workday_user_import_uki_csc_log_generation_dag = "dxctechnology_workday_user_import_uki_csc_log_generation_trial"

# Authentication URIs
AUTHS = {
    "SSO": "urn:replicon-tenant:system:authentication-type:sso",
    "Password": "urn:replicon-tenant:system:authentication-type:password"
}

# Company Code Mapper
COMPANY_CODE_MAPPER = COMPANY_CODE_MAPPING

# Scheduler settings
scheduler_interval = None  # Manual trigger for trial
schedule_interval = None  # Alias for scheduler_interval

# Connection settings
replicon_conn_id = "replicon_dxctrial01_x.workday_1"
sftp_connection_id = "sftp_useast2"
# File paths
input_file_path = "/WD/Input/UKI_CSC"
archive_file_path = "/WD/Archives/"
log_file_path = "WD/Logs/"

# Email settings
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_emails = "{{ var.value.dagrun_internal_testing_email }}"

# Instance version (for tracking)
instance_version = "trial"

# Timeoff mapper (empty for now, to be populated with actual mappings)
TIMEOFF_MAPPER = []

# Permission settings
end_user_permission = "Employee"
supervisor_end_user_permission = "Manager"
supervisor_end_user_supervision_permission = "Approver"

# Processing settings
max_active_run_process_each_users_uki_csc = 10

UDFs = UKI_CSC_UDF

# Timeoff Assignment DAGs
workday_user_import_uki_csc_process_time_off_accrual_dag = "dxctechnology_workday_user_import_v1_uki_csc_process_time_off_accrual_trial"
workday_user_import_uki_csc_user_rehire_timeoff_process_dag = "dxctechnology_workday_user_import_v1_uki_csc_user_rehire_timeoff_process_trial"
workday_user_import_uki_csc_update_user_timeoff_assignment_dag = "dxctechnology_workday_user_import_v1_uki_csc_update_user_timeoff_assignment_trial"
workday_user_import_uki_csc_ia_one_timeoff_assignment_child_dag = "dxctechnology_workday_user_import_v1_uki_csc_ia_one_timeoff_assignment_trial"
workday_user_import_uki_csc_ia_zero_timeoff_assignment_child_dag = "dxctechnology_workday_user_import_v1_uki_csc_ia_zero_timeoff_assignment_trial"

workday_user_import_process_supervisor_assignment = f"dxctechnology_workday_user_import_uki_csc_process_supervisor_assignment_child_trial"

# Batch task variable name
can_run_batch_task_var_name_uki_csc = "dxctechnology_workday_user_import_v1_uki_csc_can_run_batch_task_trial"

max_active_run_process_timeoff_no_accrual = 10

delete_future_entries_child_version = "v2"
# Cleanup child DAG ID for disabled users
delete_future_entries_child_dag_id = f"dxctechnology_workday_user_sync_delete_future_entries_child_{instance}_{delete_future_entries_child_version}"

# Overrides: bump max_active_runs to 10 for this environment (base defaults in config.py are unchanged, so production is not affected)
max_active_run_add_user_timeoff_assignment_uki_csc = 10
max_active_run_add_user_uki_csc = 10
max_active_run_update_user_uki_csc = 10
