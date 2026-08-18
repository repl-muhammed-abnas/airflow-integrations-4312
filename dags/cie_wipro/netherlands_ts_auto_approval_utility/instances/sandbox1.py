# pylint: disable=wildcard-import unused-wildcard-import
from cie_wipro.netherlands_ts_auto_approval_utility.config import *

instance = "sandbox"
environment = 'pre-production'
company_key = 'wiprosandbox1'
replicon_conn_id = 'wiprosandbox1'

team_id = 'cie'

tenant_email = "ashishtiwari@deltek.com"
internal_email = "ashishtiwari@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

timeoff_report_name = '***BaseReport_TimeOffReport'
timesheet_report_name = '***BaseReport_TimesheetApproval'

timesheet_approve_remarks = "Timesheet Approved by Approval Utility."

previous_period_in_days = 30
future_period_in_days = 0

time_zone = "America/New_York"

datetime_format = "%d.%m.%Y"  # "%m/%d/%Y"
seperator = ";"

chunk_size = 100
