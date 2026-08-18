from ipipeline.user_import_v2.config import *
from ipipeline.user_import_v2.mappers import (
    input_fields_mapper, permissions_mapper, oef_custom_mapper, time_off_type_mapper, defaults_mapper, assignment_rules_mapper, timeoff_accrual_mapper)

# Instance-specific settings
instance = 'trial'
environment = 'pre-production'
company_key = "iPipelineSB"

# Connection IDs
replicon_conn_id = 'ipipelinesb_replicon_repliconint.userimport'
sftp_conn_id = 'sftp_internal'

# SFTP File Paths - Trial (using dev paths for trial environment)
input_filepath = '/iPipeline/Dev/Input'
archive_filepath = '/iPipeline/Dev/Archive/'
log_filepath = '/iPipeline/Dev/Logs/'

reference_filepath = '/iPipeline/Dev/Reference/'
archive_reference_filepath = '/iPipeline/Dev/Reference/Archive/'
reference_filename = 'User_Import_Reference.csv'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# Batch Control Variables
can_run_batch_task_var_name = f'ipipeline_user_import_can_run_batch_task_{instance}'
can_use_reference_file_var_name = f'ipipeline_user_import_can_use_reference_file_{instance}'

version = '_v2'
dag_id_suffix = f'{instance}{version}'

# DAG IDs with instance suffix
master_dag_id = f'ipipeline_user_import_master_{dag_id_suffix}'
process_user_record_child_dag_id = f'ipipeline_user_import_process_user_child_{dag_id_suffix}'
add_user_child_dag_id = f'ipipeline_user_import_add_user_child_{dag_id_suffix}'
update_user_child_dag_id = f'ipipeline_user_import_update_user_child_{dag_id_suffix}'
disable_users_master_dag_id = f'ipipeline_user_import_disable_user_master_{dag_id_suffix}'
disable_users_child_dag_id = f'ipipeline_user_import_disable_user_child_{dag_id_suffix}'
process_log_generation_child_dag_id = f'ipipeline_user_import_log_generation_child_{dag_id_suffix}'
create_employeetypes_child_dag_id = f'ipipeline_user_import_create_employeetypes_child_{dag_id_suffix}'
create_departments_child_dag_id = f'ipipeline_user_import_create_departments_child_{dag_id_suffix}'
create_locations_child_dag_id = f'ipipeline_user_import_create_locations_child_{dag_id_suffix}'
create_projectroles_child_dag_id = f'ipipeline_user_import_create_projectroles_child_{dag_id_suffix}'
supervisor_assignment_child_dag_id = f'ipipeline_user_import_supervisor_assignment_child_{dag_id_suffix}'

timeoff_with_logic_assignment_dag_id = f'ipipeline_user_import_timeoff_with_logic_assignment_{dag_id_suffix}'

input_fields_mapper_data = input_fields_mapper.INPUT_FIELDS
permissions_mapper_data = permissions_mapper.PERMISSIONS_MAPPER
oef_field_mapper_data = oef_custom_mapper.OEF_FIELDS_MAPPER
time_off_type_mapper_data = time_off_type_mapper.TIME_OFF_TYPE_MAPPER
defaults_mapper_data = defaults_mapper.DEFAULTS_MAPPER
assignment_rules_mapper_data = assignment_rules_mapper.ASSIGNMENT_RULES_MAPPER
timeoff_accrual_mapper_data = timeoff_accrual_mapper.TIME_OFF_ACCRUAL_MAPPER