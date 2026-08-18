# pylint: disable=wildcard-import unused-wildcard-import
from horizonmedia.time_off_sync.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'
company_key = 'horizonmediagen3afmig'
replicon_conn_id = "horizonmediagen3afmig_replicon_admin"


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

schedule_interval = 300
rep_sftp_conn_id = 'sftp_horizonmedia_user_import_horizonmediagen3afmig'
client_sftp_conn_id = 'sftp_horizonmedia_user_import_horizonmediagen3afmig'

input_file_path = "/Time off Sync/Import Files"
archive_file_path = "/Time off Sync/Archive"
reference_file_path = "/Time off Sync/Import Files/Reference"
reference_archive_file_path = "/Time off Sync/Archive"
client_logs_file_path = "/Time off Sync/Log Files"
replicon_logs_file_path = "/Time Off Sync"


reference_file_name = "timeoffreference.csv"
input_file_name = ""

report_name = "User list for Integration"
disabled = True
