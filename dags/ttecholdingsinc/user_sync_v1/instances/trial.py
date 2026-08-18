# pylint: disable=wildcard-import unused-wildcard-import
from ttecholdingsinc.user_sync_v1.config import *
from ttecholdingsinc.user_sync_v1.mapper.user_sync_mapper import user_sync_mapper
from ttecholdingsinc.user_sync_v1.mapper.payrule_mapper import payrule_mapper


instance = "trial"
environment = "pre-production"

company_key = "ttecholdingsinctrial01"

replicon_conn_id = "ttecholdingsinctrial01_replicon_admin"
sftp_conn_id = "sftp_useast2"
pgp_conn_id = "pgp_ttecholdingsinctrial01_user_sync"

input_filepath = "/TTEC/Input"
archive_filepath = "/TTEC/Archive"
log_filepath = "/TTEC/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'ttec_user_sync_master_{instance}_v1'
process_groups_dagid = f'ttec_user_sync_process_groups_child_{instance}_v1'
process_new_departments_dagid = f'ttec_user_sync_process_departments_child_{instance}_v1'
process_new_servicecenter_dagid = f'ttec_user_sync_process_new_servicecenter_child_{instance}_v1'
process_users_dagid = f'ttec_user_sync_process_users_child_{instance}_v1'
process_new_users_dagid = f'ttec_user_sync_process_new_users_child_{instance}_v1'
process_update_users_dagid = f'ttec_user_sync_process_update_users_child_{instance}_v1'
processs_supervisor_dagid = f'ttec_user_sync_process_supervisors_child_{instance}_v1'
process_log_generation_dagid = f'ttec_user_sync_process_log_generation_child_{instance}_v1'
process_timeoff_type_assignment_new_user_dagid = f'ttec_user_sync_process_timeoff_type_assignment_new_user_child_{instance}_v1'


can_run_batch_task_var_name = f'ttec_user_sync_can_run_batch_task_{instance}'
can_decrypt_file_var_name = f'ttec_user_sync_can_decrypt_file_{instance}'

MAPPER = user_sync_mapper
PAYRULE_MAPPER = payrule_mapper

disabled=True
