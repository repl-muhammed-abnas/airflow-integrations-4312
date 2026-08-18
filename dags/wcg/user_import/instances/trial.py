from wcg.user_import.config import *
from wcg.user_import.mappers.defaults_mapper import defaults_mapper

instance = 'trial'
environment = 'pre-production'

company_key = "WCGafmig"
replicon_conn_id = 'wcgtrial01_replicon_admin'
sftp_conn_id = "sftp_useast2"

sftp_input_path = "/WCGSB/UserImport/Input"
sftp_archive_path = "/WCGSB/UserImport/Archive"
sftp_log_path = "/WCGSB/UserImport/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f'wcg_user_import_master_{instance}'
process_add_user_child_dag_id = f'wcg_user_import_process_add_user_child_{instance}'
process_update_user_child_dag_id = f'wcg_user_import_process_update_user_child_{instance}'
process_supervisor_child_dag_id = f'wcg_user_import_process_supervisor_child_{instance}'
process_log_generation_child_dag_id = f'wcg_user_import_process_log_generation_child_{instance}'
process_oef_dropdown_value_child_dag_id = f'wcg_user_import_process_oef_dropdown_value_child_{instance}'
process_location_group_child_dag_id = f'wcg_user_import_process_location_group_child_{instance}'

can_run_batch_task_var_name = f'wcg_user_import_can_run_batch_task_{instance}'

defaults_mapper_data = defaults_mapper
