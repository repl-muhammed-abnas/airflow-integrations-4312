# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.tiger_assignee_file_merger.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'galaxyusopcoinctrial01'
replicon_conn_id = 'galaxyusopcoinctrial01_replicon_admin'
sftp_conn_id = "sftp_useast2"
pgp_conn_id = "pgp_vialto_partners"

input_filepath = '/Tiger/Test/Input'
processing_filepath = '/Tiger/Test/Processing'
archive_filepath = '/Tiger/Test/Archive'
merge_log_filepath = '/Tiger/Test/Logs/MergeLogs'
batch_log_filepath = '/Tiger/Test/Logs/BatchLogs'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True

can_run_batch_task_var_name = f'vialto_tiger_assignee_merger_can_run_batch_task_{instance}'
