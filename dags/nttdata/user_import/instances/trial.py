# pylint: disable=wildcard-import unused-wildcard-import
from nttdata.user_import.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'nttdataafmig'
replicon_conn_id = 'nttdataafmig_replicon_replicon'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'
user_reference_data_report= '**User reference data**'

input_filepath = '/nttdata/userimport'
reference_filepath = '/nttdata/userimport/reference/'
log_filepath = '/nttdata/userimport/userimportlogs/'
archive_filepath = '/nttdata/userimport/Archive/'

can_run_batch_task = f'nttdata_user_import_can_run_batch_task_{instance}'

disable=True

disabled=True
