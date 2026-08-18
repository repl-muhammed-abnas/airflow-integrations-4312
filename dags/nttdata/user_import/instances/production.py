# pylint: disable=wildcard-import unused-wildcard-import
from nttdata.user_import.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'NTTData'
replicon_conn_id = 'nttdata_replicon_replicon'

tenant_email = 'Venu.Immadisetty@nttdata.com,David.Landry@nttdata.com,suman.tirunagari@nttdata.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'nttdata_sftp_618198'
user_reference_data_report= '**User reference data**'

input_filepath = '/Clarity Pilot/Prod'
reference_filepath = '/Clarity Pilot/Prod/Reference/'
archive_filepath = '/Clarity Pilot/Prod/Archive/'

can_run_batch_task = f'nttdata_user_import_can_run_batch_task_{instance}'
