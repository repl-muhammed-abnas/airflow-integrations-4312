# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.tiger_assignee_integration.config import *

instance = "trial"

company_key = 'galaxyusopcoinctrial01'
replicon_conn_id = 'galaxyusopcoinctrial01_replicon_admin'
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'

input_filepath = "/Tiger/Test/Processing"
archive_filepath = "/Tiger/Test/Archive"
log_filepath = "/Tiger/Test/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_decrypt_file = f'vialto_tiger_assignee_can_decrypt_file_{instance}'
can_run_batch_task_var_name = f'vialto_tiger_assignee_can_run_batch_task_{instance}'
disabled = True
