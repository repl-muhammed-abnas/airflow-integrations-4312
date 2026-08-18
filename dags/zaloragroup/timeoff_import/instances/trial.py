# pylint: disable=wildcard-import unused-wildcard-import
from zaloragroup.timeoff_import.config import *

region = 'eu-central-1'
instance = "trial"
environment = 'pre-production'
company_key = 'zaloragroupafmig'
replicon_conn_id = 'zaloragroupafmig_replicon_zrtest'
sftp_conn_id = "sftp_useast2"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
pgp_conn_id = "pgp_zaloragroup_timeoff_import"
input_filepath = '/Zaloragroup'
input_filepath_master = '/Zaloragroup/Processing'
upload_filepath = '/Zaloragroup/Processing/'
archive_filepath = '/Zaloragroup/Archive/'
log_filepath = '/Zaloragroup/Logs/TimeoffImportLogs_'
disabled = True
