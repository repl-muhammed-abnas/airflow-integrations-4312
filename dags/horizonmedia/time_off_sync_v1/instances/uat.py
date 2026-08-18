# pylint: disable=wildcard-import unused-wildcard-import
from horizonmedia.time_off_sync_v1.config import *

instance = 'uat'
region = 'us-east-1'
environment = 'pre-production'
company_key = 'horizonmediatrial01'
replicon_conn_id = "horizonmediatrial01_replicon_admin"


tenant_email = "gfraga@horizonmedia.com,Sgrandi@horizonmedia.com"
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

schedule_interval = 300
client_sftp_conn_id = 'horizonmedia_client_sftp'

input_file_path = "/Test Files/TimeOffSyncTestFiles/Import Files"
archive_file_path = "/Test Files/TimeOffSyncTestFiles/Archive"
reference_file_path = "/Test Files/TimeOffSyncTestFiles/Import Files/Reference"
reference_archive_file_path = "/Test Files/TimeOffSyncTestFiles/Archive"
client_logs_file_path = "/Test Files/TimeOffSyncTestFiles/Log Files"


reference_file_name = "timeoffreference.csv"
input_file_name = ""

report_name = "User list for Integration"
