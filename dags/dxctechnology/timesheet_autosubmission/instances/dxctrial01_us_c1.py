# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.timesheet_autosubmission.config import *

instance = 'dxctrial01'
region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sumo_conn_id = 'sumologic-exportlogger'
sftp_conn_id = 'sftp_useast2'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
exception_email = '{{ var.value.dagrun_internal_testing_email }}'
sftp_upload_path = '/timesheetautosubmission/logs/'
extract_report_name = 'Timesheets for Auto Submission - C1'
report_filter_timesheetperiod = 'TimesheetPeriodFilter'
report_filter_approvalstatus = 'ApprovalStatusFilter'
report_filter_currentdivision = 'CurrentDivisionFilter'
report_filter_employeetype = None
dag_max_active_runs = 10
dag_max_active_tasks = 128
execution_timeout_days = 14
batch_size = 50
country = 'US'
entity = 'C1'
identifier_filename = 'C1'
timesheet_status_value = '0'  # Not Submitted status
schedule = '30 6 * * FRI'
identifier_dagname = ''
employee_type = None
startdate_delta_days = 13
enddate_delta_days = 7

disable=True

disabled=True
