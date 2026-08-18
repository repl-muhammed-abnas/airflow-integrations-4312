# pylint: disable=wildcard-import unused-wildcard-import
from strayeruniversity.time_off_balance_export.config import *
instance="production"
environment="production"
replicon_conn_id="strayeruniversity-replicon-repadmin"
sftp_conn_id="sftp_strayeruniversity_550029"
company_key="StrayerUniversity"
sftp_timeoff_balance_upload_path="/Files To WorkDay/"
sftp_timeoff_balance_archive_path="/Archived/Time Off Balance Extract/"
internal_log_emails = '{{ var.value.dagrun_failure_alert_email }}'
alert_email = 'payroll@strategiced.com'
