# pylint: disable=wildcard-import unused-wildcard-import
from npsgeu.timeoff_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'npsgeuafmig'
replicon_conn_id = 'npsgeuafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'

input_filepath = '/npsgeu/timeoff_import/input'
archive_filepath = '/npsgeu/timeoff_import/archive'
log_filepath = '/npsgeu/timeoff_import/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'npsgeu_timeoff_import_{instance}_can_run_batch_task'

master_dagid = f'npsgeu_timeoff_import_master_{instance}'
process_timeoff_records_dagid = f'npsgeu_timeoff_import_process_timeoff_records_child_{instance}'
reopenedtimesheets_dagid = f'npsgeu_timeoff_import_reopenedtimesheets_child_{instance}'
disabled=True