# pylint: disable=wildcard-import unused-wildcard-import
from npsgeu.timeoff_import.config import *

instance = 'production'
environment = 'production'
company_key = 'npsgeu'
replicon_conn_id = 'npsgeu_replicon_shakeel'
sftp_conn_id = 'sftp_npsgeu_610439'

input_filepath = '/Time Off Sync/NPSG EU/PROD'
archive_filepath = '/Time Off Sync/NPSG EU/PROD/Archive'
log_filepath = '/Time Off Sync/NPSG EU/PROD/Logs'

tenant_email = '{{ var.value.dagrun_internal_log_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'npsgeu_timeoff_import_{instance}_can_run_batch_task'

master_dagid = f'npsgeu_timeoff_import_master_{instance}'
process_timeoff_records_dagid = f'npsgeu_timeoff_import_process_timeoff_records_child_{instance}'
reopenedtimesheets_dagid = f'npsgeu_timeoff_import_reopenedtimesheets_child_{instance}'
