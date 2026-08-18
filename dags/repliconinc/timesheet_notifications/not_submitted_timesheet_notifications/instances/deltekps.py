# pylint: disable=wildcard-import unused-wildcard-import
from repliconinc.timesheet_notifications.not_submitted_timesheet_notifications.config import *

instance = "deltekps"
environment = "production"

company_key = "deltekps"
replicon_conn_id = "deltekps_replicon_replicon.integration"

FROM_EMAIL_ADDR = "TCoE-APAC-Team@deltek.com"
TO_EMAIL_ADDR = "TCoE-APAC-Team@deltek.com,50eff829.deltekO365.onmicrosoft.com@amer.teams.ms"
CC_EMAIL_ADDR = "RaghuKandaswamy@deltek.com"

main_dag_id = f"replicon_not_submitted_timesheet_notifications_{instance}"

company_identifier = "Deltek Polaris"

not_submitted_timesheets_report_uri = "urn:replicon-tenant:d374ced1e022452981b5b3cc295e1651:report:d38306a8-d923-4604-a599-a3cc771649f6"
timesheet_period_filter_uri = "urn:replicon-tenant:d374ced1e022452981b5b3cc295e1651:report-filter:8a12f2442c6e4ad0b0146a416d274593;timesheetperiodfilter"
period_start_date = "04/01/2026"
