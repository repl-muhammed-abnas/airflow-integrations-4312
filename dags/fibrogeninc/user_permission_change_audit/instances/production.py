# pylint: disable=wildcard-import unused-wildcard-import
from fibrogeninc.user_permission_change_audit.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'
company_key = 'fibrogeninc'
replicon_conn_id = "FibrogenInc_replicon_SA_API_Replicon"

reference_key_name = 'Fibrogeninc/userpermissionchange/reference'
archive_key_name = 'Fibrogeninc/userpermissionchange/archive'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
