# pylint: disable=wildcard-import unused-wildcard-import
from adessa.timeoff_sync.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'adessaafmig'

replicon_conn_id = 'adessaafmig_replicon_kiran'
sftp_conn_id = 'eucentral_internal_sftp'

input_filepath = "/AdessaAfmig/ToReplicon/TimeOffSync/Input"
archive_filepath = "/AdessaAfmig/ToReplicon/TimeOffSync/Archive"
log_filepath = "/AdessaAfmig/ToReplicon/TimeOffSync/Logs"

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
