# pylint: disable=wildcard-import unused-wildcard-import
from centricbrands.user_import_v2.config import *
region = 'us-east-1'
instance = 'uat'
environment = 'pre-production'
company_key = 'centricbrandstrial02'
replicon_conn_id = 'centricbrandstrial02_admin'

tenant_email = 'laurenbrown@centricbrands.com,simprote@centricbrands.com,dlewis@centricbrands.com,jsable@centricbrands.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_centricbrands_655328'

input_filepath = '/UAT-UserSync/Input'
reference_filepath = '/UAT-UserSync/reference/'
log_filepath = '/UAT-UserSync/logs/'
archive_filepath = '/UAT-UserSync/Archived/'

can_run_batch_task = f'centricbrands_user_import_can_run_batch_task_{instance}'

disabled=True
