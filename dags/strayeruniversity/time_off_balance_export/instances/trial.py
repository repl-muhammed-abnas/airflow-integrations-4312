# pylint: disable=wildcard-import unused-wildcard-import
from strayeruniversity.time_off_balance_export.config import *
instance="trial"
replicon_conn_id="strayer_university_replicon.repadmin"
sftp_conn_id="sftp_useast2"
company_key="StrayerUniversityafmig"
sftp_timeoff_balance_upload_path="/strayer/replicon_to_workday/"
sftp_timeoff_balance_archive_path="/strayer/archive/"
internal_log_emails = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
