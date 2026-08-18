# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.timesheet_autosubmission_loa.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'dxctrial01'

replicon_conn_id = 'dxctrial01_replicon_x.replicon.workday1'
sftp_conn_id = "sftp_useast2"

tenant_email ='{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_upload_path = '/DXC/Trial/TimesheetAutoSubmissionLOA/'

master_dag_id = f'dxctechnology_timesheet_auto_submission_loa_master_{instance}'
c1_chid_dag_id = f'dxctechnology_timesheet_auto_submission_loa_each_month_c1_child_{instance}'
compass_child_dag_id = f'dxctechnology_timesheet_auto_submission_loa_each_month_compass_child_{instance}'
gsap_child_dag_id = f'dxctechnology_timesheet_auto_submission_loa_each_month_gsap_child_{instance}'
process_c1_timesheet_dag_id = f'dxctechnology_timesheet_auto_submission_loa_c1_child_{instance}'
process_compass_timesheet_dag_id = f'dxctechnology_timesheet_auto_submission_loa_compass_child_{instance}'
process_gsap_timesheet_dag_id = f'dxctechnology_timesheet_auto_submission_loa_gsap_child_{instance}'
