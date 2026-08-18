# pylint: disable=wildcard-import unused-wildcard-import
from horizonmedia.time_off_sync_v1.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'
company_key = 'horizonmediatrial01'
replicon_conn_id = "horizonmediatrial01_replicon_admin"


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

schedule_interval = 300
client_sftp_conn_id = 'sftp_useast2'

input_file_path = "/Time off Sync/Import Files"
archive_file_path = "/Time off Sync/Archive"
reference_file_path = "/Time off Sync/Import Files/Reference"
reference_archive_file_path = "/Time off Sync/Archive"
client_logs_file_path = "/Time off Sync/Log Files"


reference_file_name = "timeoffreference.csv"
input_file_name = ""

report_name = "User list for Integration"
