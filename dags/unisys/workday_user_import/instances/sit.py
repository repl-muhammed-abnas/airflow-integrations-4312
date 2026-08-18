"""
Trial Instance Configuration - Unisys Workday User Import Integration

Instance-specific configuration for the trial/dev environment of the Unisys Workday
User Import integration. This module inherits base configuration from config.py and
overrides instance-specific settings.

Configuration includes:
    - Instance identification and naming
    - Replicon connection settings
    - SFTP file transfer configuration
    - Email notification settings
    - DAG identifiers for all child DAGs
    - Feature flags and control variables

Key Settings:
    - instance: 'sit'
    - company_key: 'Unisysdev'
    - Region: us-east-1
    - SFTP paths for input, archive, and logs
    - Child DAG IDs for user processing workflows

Note:
    This is a trial instance configuration. Production settings should be
    defined in separate instance files (e.g., prod.py, sit.py).
"""
# pylint: disable=wildcard-import unused-wildcard-import
from unisys.workday_user_import.config import *

region = "us-east-1"

# Instance identification
instance = 'sit'
company_key = 'Unisysdev'  # As per design doc
replicon_conn_id = 'unisysdev_replicon_rit.workday'
pgp_conn_id = 'unisysdev_replicon_pgp_conn'

# SFTP configuration based on design doc
# Host: rsftp-useast.replicon.com
# Username: 710319
# Sandbox path: /Sandbox/Outbound/Fieldglass
sftp_conn_id = 'unisys_fieldglass_sftp_710319'
input_filepath = '/Inbound/Workday/WDImport'
archive_filepath = '/Inbound/Workday/Archive'
log_filepath = '/Inbound/Workday/Logs'

# Email configuration
tenant_email = 'Cynthia.Rachel@in.unisys.com,Srinivasa.Thota@in.unisys.com,Prashant.Vishwakarma@unisys.com,Unisysproject@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

alert_email = '{{ var.value.dagrun_internal_testing_email }}'

# DAG identifiers
master_dag = f'unisys_workday_user_import_master_{instance}'
process_each_user = f'unisys_workday_user_import_process_users_child_{instance}'
process_new_users = f'unisys_workday_user_import_process_new_users_child_{instance}'
process_update_users = f'unisys_workday_user_import_process_update_users_child_{instance}'
processs_supervisor = f'unisys_workday_user_import_processs_supervisor_child_{instance}'
process_log_generation = f'unisys_workday_user_import_process_log_generation_child_{instance}'


process_groups_dag_id = f'unisys_workday_user_import_process_groups_child_{instance}'
process_new_locations = f'unisys_workday_user_import_process_locations_child_{instance}'
process_new_departments = f'unisys_workday_user_import_process_departments_child_{instance}'
process_new_usertypes = f'unisys_workday_user_import_process_usertypes_child_{instance}'
process_update_divisions = f'unisys_workday_user_import_co_code_costcenter_child_{instance}'
process_new_schedule = f'unisys_workday_user_import_process_new_schedule_child_{instance}'
process_disable_users = f'unisys_workday_user_import_process_disable_users_child_{instance}'
process_projects = f'unisys_workday_user_import_process_projects_child_{instance}'

can_decrypt_file_var_name = f'unisys_workday_user_import_can_decrypt_file_var_name_{instance}'
can_run_batch_task = f'unisys_workday_user_import_can_run_batch_task_var_name_{instance}'
# disabled = True
