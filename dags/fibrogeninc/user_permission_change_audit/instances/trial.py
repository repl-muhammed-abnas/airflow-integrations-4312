# pylint: disable=wildcard-import unused-wildcard-import
from fibrogeninc.user_permission_change_audit.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'
company_key = 'fibrogenincafmig'
replicon_conn_id = "fibrogenincafmig_replicon_SA_API_Replicon"

reference_key_name = 'Fibrogenincafmig/userpermissionchange/reference'
archive_key_name = 'Fibrogenincafmig/userpermissionchange/archive'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
