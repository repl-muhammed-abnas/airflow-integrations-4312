# pylint: disable=wildcard-import unused-wildcard-import
from cie_darkmattertechnologies.ts_submit_utility.config import *

region = 'us-east-1'
environment = 'production'
instance = 'prod'
company_key = 'DarkMatterTechnologiesLLC'

replicon_conn_id = 'DarkMatterTechnologiesLLC'

tenant_email = 'jarod.magdon@dmatter.com, heidi.silverstone@dmatter.com'
internal_logs_email = "PrakharAgrawal@deltek.com, ashishtiwari@deltek.com, aravindgalipalli@deltek.com, abhishekboda@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


can_run_batch_task_var_name = f'{company_key}_timesheet_submission_{instance}_can_run_batch_task'.lower()

location = ""

params = f"{company_key}_config_variables_{instance}".lower()
schedule_interval = "0 2 * * 1-5"
timezone = "America/Denver"


ts_report_name = "***BaseReportTimeSheet"
te_report_name = "***BaseReportTimeEntries"