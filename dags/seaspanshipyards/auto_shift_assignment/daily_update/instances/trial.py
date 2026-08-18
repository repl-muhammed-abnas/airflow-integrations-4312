# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_shift_assignment.daily_update.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'seaspanshipyardsafmig'
user_shift_report_name = "**Shift Assignment Report**"

schedule_interval_weekly = "0 14 * * 1,2,3,4"
replicon_conn_id = 'seaspanshipyardsafmig_replicon_admin'
disabled = True
