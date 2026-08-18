# pylint: disable=wildcard-import unused-wildcard-import
from centricbrands.user_import_v2.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'CentricBrands'
replicon_conn_id = 'centricbrands_replicon_admin'

tenant_email = "laurenbrown@centricbrands.com,simprote@centricbrands.com,dlewis@centricbrands.com,jsable@centricbrands.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_centricbrands_655328'

input_filepath = '/UserSync/Input'
reference_filepath = '/UserSync/reference/'
log_filepath = '/UserSync/logs/'
archive_filepath = '/UserSync/Archived/'

can_run_batch_task = f'centricbrands_user_import_can_run_batch_task_{instance}'
