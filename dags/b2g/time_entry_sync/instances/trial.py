# pylint: disable=wildcard-import unused-wildcard-import
from b2g.time_entry_sync.config import *

environment = 'pre-production'
instance = "trial"
company_key = "wrdttrial01"

sftp_conn_id = "wrdttrial01_sftp_664944"

replicon_conn_id = "wrdttrial01_replicon_admin"

input_filepath = "/UAT/Time_sync/Input"
archive_filepath = "/UAT/Time_sync/Archive"
log_filepath = "/UAT/Time_sync/Logs"

tenant_email = 'ken.cave@westregion.team'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
