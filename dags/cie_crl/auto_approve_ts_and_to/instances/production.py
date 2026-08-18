# pylint: disable=wildcard-import unused-wildcard-import
from cie_crl.auto_approve_ts_and_to.config import *

environment = "production"
instance = "production"
region = 'us-east-1'
company_key = "CharlesRiverLaboratories"
team_id = "CIE"
replicon_conn_id = "crl_conn_id_prod"
report_filter_entrydate = "TimesheetPeriodFilter"

timesheet_status_filter_uri = 'ApprovalStatusFilter'

holiday_calender_name = "Timesheet Auto Approval Calendar - Canada"
email_distro_list = 'MTL-Payroll@crl.com'
internal_logs_email = "PrakharAgrawal@deltek.com, ashishtiwari@deltek.com, SakshiBhumkar@deltek.com, PIEReplicon@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
timesheet_approve_remarks = "Timesheet Approved by Approval Utility."

days = 40

max_child_run = 3
execution_timeout_days = 14

location = ""
can_run_batch_task_var_name = 'true'
is_not_submitted = "is not submitted"
is_waiting_for_approval = "is waiting for approval"

schedule_interval = "15 16 * * *"
time_zone = "US/Eastern"
date_format =  "%m/%d/%Y"
report_date_format = '%b %d, %Y'

ts_report_name = "TS_TO_autoapproval - Canada"
to_report_name = "TimeOff Booking Details - Canada"
