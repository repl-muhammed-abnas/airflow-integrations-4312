# pylint: disable=wildcard-import unused-wildcard-import
from repliconinc.timesheet_notifications.not_submitted_timesheet_notifications.config import *

instance = "repliconpinc"
environment = "production"

company_key = "repliconpinc"
replicon_conn_id = "repliconpinc_replicon_replicon.integration"

FROM_EMAIL_ADDR = "TCoE-APAC-Team@deltek.com"
TO_EMAIL_ADDR = "TCoE-APAC-Team@deltek.com,50eff829.deltekO365.onmicrosoft.com@amer.teams.ms"
CC_EMAIL_ADDR = "RaghuKandaswamy@deltek.com"

main_dag_id = f"replicon_not_submitted_timesheet_notifications_{instance}"

company_identifier = "Replicon Polaris"

not_submitted_timesheets_report_uri = "urn:replicon-tenant:115e953e9f1744d7bdc12152f9d4a1b4:report:b616dc35-aa40-462c-a516-19b4c9a24591"
timesheet_period_filter_uri = "urn:replicon-tenant:115e953e9f1744d7bdc12152f9d4a1b4:report-filter:8a12f2442c6e4ad0b0146a416d274593;timesheetperiodfilter"
period_start_date = "03/29/2026"
