# pylint: disable=wildcard-import unused-wildcard-import
from centricbrands.user_import_v2.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'centricbrandstrial01'
replicon_conn_id = 'centricbrandstrial01_replicon_admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_centricbrands_eucentral'

input_filepath = '/centricbrands/Input'
reference_filepath = '/centricbrands/Reference/'
log_filepath = '/centricbrands/Logs/'
archive_filepath = '/centricbrands/Archive/'

can_run_batch_task = f'centricbrands_user_import_can_run_batch_task_{instance}'

disabled=True
