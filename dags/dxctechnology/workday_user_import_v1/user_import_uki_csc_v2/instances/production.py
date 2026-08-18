# UK&I CSC Production Instance Configuration
from dxctechnology.workday_user_import_v1.user_import_uki_csc_v2.config import *
from dxctechnology.workday_user_import_v1.user_import_uki_csc_v2.mapper.authentication_and_product import PRODUCT
from dxctechnology.workday_user_import_v1.user_import_uki_csc_v2.mapper.company_code_mapper import COMPANY_CODE_MAPPER as COMPANY_CODE_MAPPING
from dxctechnology.workday_user_import_v1.user_import_uki_csc_v2.mapper.activity_mapper import FTP_ACTIVITY_MAPPER
from dxctechnology.workday_user_import_v1.user_import_uki_csc_v2.mapper.csc_assignment_mapper_v4 import CSC_ASSIGNMENT_MAPPER as CSC_ASSIGNMENT_MAPPER_CSC

from datetime import timedelta

instance = "prod"
environment = "production"

PRODUCT = PRODUCT
company_key = "dxctechnology"

version: str = "_v2"
instance_version = f"_{instance}{version if version else ''}"
description_version_suffix = version.replace('_', '') if version else ''

# DAG IDs
workday_user_import_process_uki_csc_data_child_dag = f"dxctechnology_workday_user_import_master_uki_csc{instance_version}"
workday_user_import_process_uki_csc_data_child_dag_description = f"DXC Workday User Import UKI CSC - Process Data MASTER {description_version_suffix}"

workday_user_import_process_uki_csc_user_records_child_dag = f"dxctechnology_workday_user_import_uki_csc_process_users{instance_version}"
workday_user_import_process_uki_csc_user_records_child_dag_description = f"DXC Workday User Import UKI CSC - Process Users Child DAG {description_version_suffix}"

workday_user_import_uki_csc_add_user_dag = f"dxctechnology_workday_user_import_uki_csc_add_user{instance_version}"
workday_user_import_uki_csc_add_user_dag_description = f"DXC Workday User Import UKI CSC - Process Add User {description_version_suffix}"

workday_user_import_uki_csc_update_user_dag = f"dxctechnology_workday_user_import_uki_csc_update_user{instance_version}"
workday_user_import_uki_csc_update_user_dag_description = f"DXC Workday User Import UKI CSC - Process Update User {description_version_suffix}"

workday_user_import_uki_csc_add_user_timeoff_assignment_dag = f"dxctechnology_workday_user_import_uki_csc_add_user_timeoff_assignment{instance_version}"
workday_user_import_uki_csc_add_user_timeoff_assignment_dag_description = f"DXC Workday User Import UKI CSC - Process Add User Timeoff Assignment {description_version_suffix}"

workday_user_import_uki_csc_user_rehire_timeoff_process_dag = f"dxctechnology_workday_user_import_uki_csc_user_rehire_timeoff{instance_version}"
workday_user_import_uki_csc_user_rehire_timeoff_process_dag_description = f"DXC Workday User Import UKI CSC - Process Rehire User Timeoff {description_version_suffix}"

workday_user_import_uki_csc_update_user_timeoff_assignment_dag = f"dxctechnology_workday_user_import_uki_csc_update_user_timeoff_assignment{instance_version}"
workday_user_import_uki_csc_update_user_timeoff_assignment_dag_description = f"DXC Workday User Import UKI CSC - Process Update User Timeoff Assignment {description_version_suffix}"

workday_user_import_uki_csc_log_generation_dag = f"dxctechnology_workday_user_import_uki_csc_log_generation{instance_version}"
workday_user_import_uki_csc_log_generation_dag_description = f"DXC Workday User Import UKI CSC - Process Log Generation {description_version_suffix}"

# Authentication URIs
AUTHS = {
    "SSO": "urn:replicon-tenant:system:authentication-type:sso",
    "Password": "urn:replicon-tenant:system:authentication-type:password"
}

# Company Code Mapper
COMPANY_CODE_MAPPER = COMPANY_CODE_MAPPING

# Assignment Mapper (production uses v3)
CSC_ASSIGNMENT_MAPPER = CSC_ASSIGNMENT_MAPPER_CSC

# Scheduler settings
schedule_interval = timedelta(seconds=30)

# Connection settings
replicon_conn_id = "replicon_dxctechnology_x.workday_1"
sftp_connection_id = "sftp_dxctechnology_628172_Workday"
# File paths
input_file_path = "/Production/Input/UKI_CSC"
archive_file_path = "/Production/Archives/"
log_file_path = "/Production/Logs/"

# Email settings
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# Timeoff mapper (not used - timeoff handled via timeoff_mapper.py)
TIMEOFF_MAPPER = []

# Permission settings
end_user_permission = "Employee"
supervisor_end_user_permission = "Manager"
supervisor_end_user_supervision_permission = "Approver"

UDFs = UKI_CSC_UDF

# Timeoff Assignment DAGs
workday_user_import_uki_csc_process_time_off_accrual_dag = f"dxctechnology_workday_user_import_uki_csc_process_time_off_accrual{instance_version}"
workday_user_import_uki_csc_process_time_off_accrual_dag_description = f"DXC Workday User Import UKI CSC - Process Time Off Accrual {description_version_suffix}"

workday_user_import_uki_csc_ia_one_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_uki_csc_ia_one_timeoff_assignment{instance_version}"
workday_user_import_uki_csc_ia_one_timeoff_assignment_child_dag_description = f"DXC Workday User Import UKI CSC - IA One Timeoff Assignment {description_version_suffix}"

workday_user_import_uki_csc_ia_zero_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_uki_csc_ia_zero_timeoff_assignment{instance_version}"
workday_user_import_uki_csc_ia_zero_timeoff_assignment_child_dag_description = f"DXC Workday User Import UKI CSC - IA Zero Timeoff Assignment {description_version_suffix}"

workday_user_import_process_supervisor_assignment = f"dxctechnology_workday_user_import_uki_csc_process_supervisor_assignment_child{instance_version}"
workday_user_import_process_supervisor_assignment_description = f"DXC Workday User Import UKI CSC - Process Supervisor Assignment {description_version_suffix}"

# Batch task variable name
can_run_batch_task_var_name_uki_csc = f"dxctechnology_workday_user_import_uki_csc_can_run_batch_task{instance_version}"

max_active_run_process_timeoff_no_accrual = 5

# Cleanup child DAG ID for disabled users
delete_future_entries_child_dag_id = f"dxctechnology_workday_user_sync_delete_future_entries_child_production_v2"

max_active_run_add_user_timeoff_assignment_uki_csc = 20
max_active_run_add_user_uki_csc = 10
max_active_run_process_timeoff_no_accrual = 15
max_active_run_process_each_users_uki_csc = 10
max_active_run_update_user_uki_csc = 10
max_active_run_update_user_timeoff = 20

ACTIVITY_MAPPER = FTP_ACTIVITY_MAPPER
