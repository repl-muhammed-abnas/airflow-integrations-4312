# pylint: disable=wildcard-import unused-wildcard-import
from repliconinc.timesheet_approval_in_polaris_for_40_hrs.config import *

environment = 'pre-production'
instance = "trial"
company_key = "Repliconpincstream6dev"

replicon_conn_id = "repliconinc_replicon_replicon.integration"

FROM_EMAIL_ADDR = "integrationteam@replicon.com"
TO_EMAIL_ADDR = "RepliconIntegrationTeam@deltek.com,TeamDice@deltek.com,dd4e5f09.deltekO365.onmicrosoft.com@amer.teams.ms,5a81ad83.deltekO365.onmicrosoft.com@amer.teams.ms"
CC_EMAIL_ADDR = "RaghuKandaswamy@deltek.com"

timesheet_report_all_timeoff = 'Report for timesheet submission for all timeoffs'


master_dag = f"repliconinc_auto_timesheet_approval_in_polaris_for_40hrs_master_{instance}"
process_timesheet_approval_child_dag = f"repliconinc_auto_timesheet_approval_in_polaris_for_40hrs_child_{instance}"



