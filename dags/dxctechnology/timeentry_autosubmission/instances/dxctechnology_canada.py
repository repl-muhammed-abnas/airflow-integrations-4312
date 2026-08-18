# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.timeentry_autosubmission.config import *
from dxctechnology.timeentry_autosubmission.mappers.dxc_timeentry_auto_submission_location_mapper import timeentry_auto_submission_location_mapper

instance = "dxctechnology_canada"

region = 'us-east-2'
environment = 'production'
company_key = 'dxctechnology'

pacific_timezone = 'America/Denver'

sftp_conn_id = 'DXCTechnology-sftp-628172_C1'

replicon_conn_id = "dxctechnology-replicon-timeentryautosubmission"
execution_timeout_days = 14

report_name = 'TimeEntrySubmission_For_All_Locations'

log_filepath = '/Production/Outbound/Time Entry Auto Submission Log/'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

country = "Canada"
project_dag_max_active_runs = 3

MAPPER = timeentry_auto_submission_location_mapper
