# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.adhoc.tiger_assignee_file_merger_adhoc.config import *

instance = 'production'
environment = 'production'

company_key = 'GalaxyUSOpcoInc'
replicon_conn_id = 'galaxyusopcoinc_replicon_admin'
sftp_conn_id = "sftp_galaxyusopcoinc_676273"
pgp_conn_id = "pgp_vialto_partners"

input_filepath = '/Tiger/Prod/Adhoc'
processing_filepath = '/Tiger/Prod/Processing'
archive_filepath = '/Tiger/Prod/Archive'
merge_log_filepath = '/Tiger/Prod/Logs/MergeLogs'
batch_log_filepath = '/Tiger/Prod/Logs/BatchLogs'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'vialto_tiger_assignee_merger_can_run_batch_task_{instance}'
