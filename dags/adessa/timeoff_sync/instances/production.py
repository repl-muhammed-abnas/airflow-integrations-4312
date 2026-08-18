# pylint: disable=wildcard-import unused-wildcard-import
from adessa.timeoff_sync.config import *

instance = 'production'
environment = 'production'

company_key = 'adessa'

replicon_conn_id = 'adessa_replicon_kiran.r@replicon.com'
sftp_conn_id = 'sftp_adessa_P9817_SFTP'

input_filepath = "/ToReplicon/TimeOffSync/Input"
archive_filepath = "/ToReplicon/TimeOffSync/Archive"
log_filepath = "/ToReplicon/TimeOffSync/Logs"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
