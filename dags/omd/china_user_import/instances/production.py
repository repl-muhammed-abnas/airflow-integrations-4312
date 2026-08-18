# pylint: disable=wildcard-import unused-wildcard-import
from omd.china_user_import.config import *
region = 'eu-central-1'
instance = 'production'
environment = 'production'
company_key = 'OMDSingaporePteLtd'
replicon_conn_id = 'OMDSingaporePteLtd_replicon_admin'

tenant_email = 'timesheet.omgchina@omnicommediagroup.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_OMDSingaporePteLtd_660053'

input_filepath = '/OMG China User Import Production/Input'
reference_filepath = '/OMG China User Import Production/Reference/'
log_filepath = '/OMG China User Import Production/Logs/'
archive_filepath = '/OMG China User Import Production/Archive/'

can_run_batch_task = f'omdsingaporepteltd_user_import_can_run_batch_task_{instance}'
