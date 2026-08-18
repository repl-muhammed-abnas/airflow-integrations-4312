# pylint: disable=wildcard-import unused-wildcard-import
from cie_capgemini.timesheet_auto_approval_australia.config import *

environment = 'pre-production'
instance = "uat"
company_key = 'CapgeminiUAT'
replicon_conn_id = 'CapgeminiUAT'

team_id = 'cie'

country = 'australia'

tenant_email = 'ashishtiwari@deltek.com'
internal_email = "ashishtiwari@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

timeoff_report_name = '***BaseReport_TimeOffReport_Australia'
timesheet_report_name = '***BaseReport_TimesheetApproval_Australia'

timesheet_approve_remarks = "Timesheet Approved by Approval Utility."

previous_period_in_months = 3
future_period_in_months = 1

time_zone = "America/New_York"
dateFormat = '%m/%d/%Y'
dateFormatTSReport = '%b, %d, %Y'
dateFormatTimeOffReport = '%b %d, %Y'

chunk_size = 200

disable=True

disabled=True