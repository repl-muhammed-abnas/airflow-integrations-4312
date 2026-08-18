# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.timesheet_autosubmission_loa.config import *

instance = 'dxcsandbox'
environment = 'pre-production'

company_key = 'DXCSandbox'

replicon_conn_id = 'dxcsandbox_replicon_autosubmissionloa'
sftp_conn_id = "sftp_dxcsandbox_628172"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_upload_path = '/Sandbox/TimesheetAutoSubmissionLOA/Logs/'

master_dag_id = f'dxctechnology_timesheet_auto_submission_loa_master_{instance}'
c1_chid_dag_id = f'dxctechnology_timesheet_auto_submission_loa_each_month_c1_child_{instance}'
compass_child_dag_id = f'dxctechnology_timesheet_auto_submission_loa_each_month_compass_child_{instance}'
gsap_child_dag_id = f'dxctechnology_timesheet_auto_submission_loa_each_month_gsap_child_{instance}'
process_c1_timesheet_dag_id = f'dxctechnology_timesheet_auto_submission_loa_c1_child_{instance}'
process_compass_timesheet_dag_id = f'dxctechnology_timesheet_auto_submission_loa_compass_child_{instance}'
process_gsap_timesheet_dag_id = f'dxctechnology_timesheet_auto_submission_loa_gsap_child_{instance}'
