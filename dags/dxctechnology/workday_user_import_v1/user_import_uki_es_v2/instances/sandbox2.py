# UK&I ES Sandbox2 Instance Configuration
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.config import *
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.mapper.authentication_and_product import PRODUCT
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.mapper.company_code_mapper import COMPANY_CODE_MAPPER as COMPANY_CODE_MAPPING
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.mapper.es_assignment_mapper_v5 import DXC_ASSIGNMENT_MAPPER as DXC_ASSIGNMENT_MAPPER_ES
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.mapper.activity_mapper import ACTIVITY_MAPPER
from datetime import timedelta
instance = "sandbox2"

PRODUCT = PRODUCT
company_key = "dxcsandbox2"

version: str = "_v2"
instance_version = f"_{instance}{version if version else ''}"
description_version_suffix = version.replace('_', '') if version else ''

# DAG IDs
workday_user_import_process_uki_es_data_child_dag = f"dxctechnology_workday_user_import_master_uki_es{instance_version}"
workday_user_import_process_uki_es_data_child_dag_description = f"DXC Workday User Import UKI ES - Process Data MASTER {description_version_suffix}"

workday_user_import_process_uki_es_user_records_child_dag = f"dxctechnology_workday_user_import_uki_es_process_users{instance_version}"
workday_user_import_process_uki_es_user_records_child_dag_description = f"DXC Workday User Import UKI ES - Process Users Child DAG {description_version_suffix}"

workday_user_import_uki_es_add_user_dag = f"dxctechnology_workday_user_import_uki_es_add_user{instance_version}"
workday_user_import_uki_es_add_user_dag_description = f"DXC Workday User Import UKI ES - Process Add User {description_version_suffix}"

workday_user_import_uki_es_update_user_dag = f"dxctechnology_workday_user_import_uki_es_update_user{instance_version}"
workday_user_import_uki_es_update_user_dag_description = f"DXC Workday User Import UKI ES - Process Update User {description_version_suffix}"

workday_user_import_uki_es_add_user_timeoff_assignment_dag = f"dxctechnology_workday_user_import_uki_es_add_user_timeoff_assignment{instance_version}"
workday_user_import_uki_es_add_user_timeoff_assignment_dag_description = f"DXC Workday User Import UKI ES - Process Add User Timeoff Assignment {description_version_suffix}"

workday_user_import_uki_es_user_rehire_timeoff_process_dag = f"dxctechnology_workday_user_import_uki_es_user_rehire_timeoff{instance_version}"
workday_user_import_uki_es_user_rehire_timeoff_process_dag_description = f"DXC Workday User Import UKI ES - Process Rehire User Timeoff {description_version_suffix}"

workday_user_import_uki_es_update_user_timeoff_assignment_dag = f"dxctechnology_workday_user_import_uki_es_update_user_timeoff_assignment{instance_version}"
workday_user_import_uki_es_update_user_timeoff_assignment_dag_description = f"DXC Workday User Import UKI ES - Process Update User Timeoff Assignment {description_version_suffix}"

workday_user_import_uki_es_log_generation_dag = f"dxctechnology_workday_user_import_uki_es_log_generation{instance_version}"
workday_user_import_uki_es_log_generation_dag_description = f"DXC Workday User Import UKI ES - Process Log Generation {description_version_suffix}"

# Authentication URIs
AUTHS = {
    "SSO": "urn:replicon-tenant:system:authentication-type:sso",
    "Password": "urn:replicon-tenant:system:authentication-type:password"
}

# Company Code Mapper
COMPANY_CODE_MAPPER = COMPANY_CODE_MAPPING
DXC_ASSIGNMENT_MAPPER = DXC_ASSIGNMENT_MAPPER_ES
ACTIVITY_MAPPER_ES = ACTIVITY_MAPPER

# Scheduler settings
scheduler_interval = None  # Manual trigger for sandbox2
schedule_interval = timedelta(seconds =30)  # Alias for scheduler_interval

# Connection settings
replicon_conn_id = "replicon_dxcsandbox2_x.workday_1"
sftp_connection_id = "sftp_dxcsandbox2_628172_Workday"
# File paths
input_file_path = "/Test/Input/UKI_ES"
archive_file_path = "/Test/Archives/"
log_file_path = "/Test/Logs/"

# Email settings
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_emails = "{{ var.value.dagrun_internal_testing_email }}"

# Timeoff mapper (empty for now, to be populated with actual mappings)

# Permission settings
end_user_permission = "Employee"
supervisor_end_user_permission = "Manager"
supervisor_end_user_supervision_permission = "Approver"

# Processing settings
max_active_run_process_each_users_uki_es = 10

UDFs = uki_es_UDF

# Timeoff Assignment DAGs
workday_user_import_uki_es_process_time_off_accrual_dag = f"dxctechnology_workday_user_import_uki_es_process_time_off_accrual{instance_version}"
workday_user_import_uki_es_process_time_off_accrual_dag_description = f"DXC Workday User Import UKI ES - Process Time Off Accrual {description_version_suffix}"

workday_user_import_uki_es_ia_one_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_uki_es_ia_one_timeoff_assignment{instance_version}"
workday_user_import_uki_es_ia_one_timeoff_assignment_child_dag_description = f"DXC Workday User Import UKI ES - IA One Timeoff Assignment {description_version_suffix}"

workday_user_import_uki_es_ia_zero_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_uki_es_ia_zero_timeoff_assignment{instance_version}"
workday_user_import_uki_es_ia_zero_timeoff_assignment_child_dag_description = f"DXC Workday User Import UKI ES - IA Zero Timeoff Assignment {description_version_suffix}"

workday_user_import_process_supervisor_assignment = f"dxctechnology_workday_user_import_uki_es_process_supervisor_assignment_child{instance_version}"
workday_user_import_process_supervisor_assignment_description = f"DXC Workday User Import UKI ES - Process Supervisor Assignment {description_version_suffix}"

# Batch task variable name
can_run_batch_task_var_name_uki_es = f"dxctechnology_workday_user_import_uki_es_can_run_batch_task{instance_version}"

max_active_run_process_timeoff_no_accrual = 10

# Cleanup child DAG ID for disabled users
delete_future_entries_child_dag_id = f"dxctechnology_workday_user_sync_delete_future_entries_child_{instance}_v2"

# Overrides: bump max_active_runs to 10 for this environment (base defaults in config.py are unchanged, so production is not affected)
max_active_run_add_user_timeoff_assignment_uki_es = 10
max_active_run_add_user_uki_es = 10
max_active_run_update_user_timeoff = 10
max_active_run_update_user_uki_es = 10
