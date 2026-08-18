# pylint: disable=wildcard-import unused-wildcard-import
from cie_darkmattertechnologies.ts_submit_utility.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'DarkMatterTechnologiesLLCTrial01'
replicon_conn_id = 'darkmattertechnologiesllctrial01_conn_id'


tenant_email = 'PrakharAgrawal@deltek.com, ashishtiwari@deltek.com'
internal_logs_email = "PrakharAgrawal@deltek.com, ashishtiwari@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


can_run_batch_task_var_name = f'{company_key}_timesheet_submission_{instance}_can_run_batch_task'.lower()

location = ""

params = f"{company_key}_config_variables_{instance}".lower()
schedule_interval = "0 2 * * 1-5"
timezone = "America/Denver"


ts_report_name = "***BaseReportTimeSheet"
te_report_name = "***BaseReportTimeEntries"