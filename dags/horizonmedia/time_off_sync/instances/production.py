# pylint: disable=wildcard-import unused-wildcard-import
from horizonmedia.time_off_sync.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'
company_key = 'Horizonmedia'
replicon_conn_id = "horizonmedia_repliconadmin_replicon"


tenant_email = "gfraga@horizonmedia.com,Sgrandi@horizonmedia.com"
bcc_tenant_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

schedule_interval = 30
rep_sftp_conn_id = 'replicon_horizon_sftp'
client_sftp_conn_id = 'horizonmedia_client_sftp'

input_file_path = "/Time off Sync/Import Files"
archive_file_path = "/Time off Sync/Archive"
reference_file_path = "/Time off Sync/Import Files/Reference"
reference_archive_file_path = "/Time off Sync/Archive"
client_logs_file_path = "/Time off Sync/Log Files"
replicon_logs_file_path = "/Time Off Sync"


reference_file_name = "timeoffreference.csv"

report_name = "User list for Integration"
