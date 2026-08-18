# pylint: disable=wildcard-import unused-wildcard-import
from cie_capgemini.timesheet_auto_approval.config import *

environment = 'production'
instance = "prod"
company_key = 'Capgemini'
replicon_conn_id = 'CIE_Capgemini'

team_id = 'cie'

tenant_email = 'ashishtiwari@deltek.com'
internal_email = "ashishtiwari@deltek.com,SumanaVikraman@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

timeoff_report_name = '***BaseReport_TimeOffReport'
timesheet_report_name = '***BaseReport_TimesheetApproval'

timesheet_approve_remarks = "Timesheet Approved by Approval Utility."

previous_period_in_months = 3
future_period_in_months = 1

time_zone = "America/New_York"


chunk_size = 200
