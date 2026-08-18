# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.timeentry_autosubmission.config import *
from dxctechnology.timeentry_autosubmission.mappers.dxc_timeentry_auto_submission_location_mapper import timeentry_auto_submission_location_mapper

instance = "dxcsandbox2_europe"

region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxcsandbox2'

pacific_timezone = 'Europe/Brussels'

sftp_conn_id = 'sftp_dxcsandbox2_c1'

replicon_conn_id = "dxcsandbox2-replicon-timeentryautosubmission"
execution_timeout_days = 14

report_name = 'TimeEntrySubmission_For_All_Locations'

log_filepath = '/Test/Outbound/Time Entry Auto Submission Log/'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

country = "Europe"
project_dag_max_active_runs = 10

MAPPER = timeentry_auto_submission_location_mapper
