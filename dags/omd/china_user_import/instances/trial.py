# pylint: disable=wildcard-import unused-wildcard-import
from omd.china_user_import.config import *
region = 'eu-central-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'OMDSingaporePteLtdafmig'
replicon_conn_id = 'omdsingaporepteltdafmig_replicon_admin'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'

input_filepath = '/OMD'
reference_filepath = '/OMD/Reference/'
log_filepath = '/OMD/Logs/'
archive_filepath = '/OMD/Archive/'

can_run_batch_task = f'omdsingaporepteltd_user_import_can_run_batch_task_{instance}'

disabled = True
