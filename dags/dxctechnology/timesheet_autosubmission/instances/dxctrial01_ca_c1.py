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
row_threshold = 400000
country = 'Canada'
entity = 'C1'
identifier_filename = 'C1'
employee_type = None
timesheet_status_value = '0'
schedule = '30 6 * * FRI'
identifier_dagname = ''
startdate_delta_days = 12
enddate_delta_days = 6

disable=True

disabled=True
