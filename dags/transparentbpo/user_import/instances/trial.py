"""
TransparentBPO User Import Trial Instance Configuration
"""
from transparentbpo.user_import.config import *
from transparentbpo.user_import.mappers.holiday_timezone_mapper import holiday_and_timezone_mapper
from transparentbpo.user_import.mappers.payrule_mapper import payrule_mapper
from transparentbpo.user_import.mappers.time_off_mapper import time_off_mapper

from transparentbpo.project_and_task_sync.instances.trial import process_logs_pregeneration_dag_id, master_dag_id as project_task_sync_master_dag_id

# AWS Configuration
instance = 'trial'
environment = 'pre-production'

# Instance Identification
company_key = "TransparentBPOafmig"

# Connection IDs
replicon_conn_id = 'transparentbpoafmig_replicon_admin'
bamboohr_conn_id = 'transparentbpoafmig_bamboo_basicauth'
sftp_conn_id = 'sftp_useast2'

# Email configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'


# File paths for SFTP
archive_filepath = '/transparentbpo/user_import/archive'
reference_filepath = '/transparentbpo/user_import/reference'
log_filepath = '/transparentbpo/user_import/logs'

version = ''  # _v1, _v2
dag_id_suffix = f"{instance}{version}"

# DAG IDs
master_dag_id = f"transparentbpo_user_import_master_{dag_id_suffix}"
process_each_user_dag_id = f"transparentbpo_user_import_process_each_user_child_{dag_id_suffix}"

process_update_user_dag_id = f"transparentbpo_user_import_process_update_user_child_{dag_id_suffix}"
process_add_user_dag_id = f"transparentbpo_user_import_process_add_user_child_{dag_id_suffix}"
process_add_new_supervisor_dag_id = f"transparentbpo_user_import_process_add_new_supervisor_child_{dag_id_suffix}"

process_log_generation_dag_id = f"transparentbpo_user_import_process_log_generation_child_{dag_id_suffix}"

can_run_batch_task_var_name = f"transparentbpo_user_import_{instance}_can_run_batch_task"

bamboo_user_changes_lookback_timestamp = f"transparentbpo_user_import_bamboo_lookback_timestamp_{instance}"

#Project task sync DAG IDs
process_project_task_creation_dag_id = project_task_sync_master_dag_id
process_project_logs_pregeneration_dag_id = process_logs_pregeneration_dag_id

HOLIDAY_AND_TIMEZONE_MAPPER = holiday_and_timezone_mapper
PAYRULE_MAPPER = payrule_mapper
TIME_OFF_MAPPER = time_off_mapper
