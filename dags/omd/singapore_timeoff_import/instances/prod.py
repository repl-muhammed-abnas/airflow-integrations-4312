# pylint: disable=wildcard-import unused-wildcard-import
from omd.singapore_timeoff_import.config import *

environment = 'production'
instance = 'prod'
company_key = 'omdsingaporepteltd'
replicon_conn_id = 'OMDSingaporePteLtd_replicon_admin'
sftp_conn_id = 'sftp_OMDSingaporePteLtd_660053'

input_filepath = '/Time off Import/Input/'
reference_filepath = '/Time off Import/Reference/'
archive_filepath = '/Time off Import/Archive/'
log_filepath = '/Time off Import/Logs/'
processing_filepath = '/Time off Import/Processing'

tenant_email = 'omg-sg-repliconsupport@omnicommediagroup.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


can_run_batch_task_var_name = f'omd_timeoff_import_{instance}_can_run_batch_task'
