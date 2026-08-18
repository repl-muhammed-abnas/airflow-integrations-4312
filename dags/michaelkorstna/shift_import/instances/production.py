# pylint: disable=wildcard-import unused-wildcard-import
from michaelkorstna.shift_import.config import *

instance = 'production'
environment = 'production'

company_key = 'MichaelKorsTnA'
replicon_conn_id = 'MichaelKorsTnA_replicon_radmin'
sftp_conn_id = 'sftp_MichaelKorsTnA_648665'

input_filepath = '/PROD/From MK'
archive_filepath = '/PROD/Archive'
log_filepath = '/PROD/Logs'
shifts_filepath = '/PROD/Archive'

tenant_email = 'EUOperations@MichaelKors.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'michaelkorstna_shift_import_{instance}_can_run_batch_task'
