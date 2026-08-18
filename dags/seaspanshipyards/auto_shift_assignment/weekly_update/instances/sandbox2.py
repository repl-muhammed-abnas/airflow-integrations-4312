# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_shift_assignment.weekly_update.config import *

instance = 'sandbox2'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'SeaspanShipyardsOra'
user_shift_report_name = "**Shift Assignment Report**"

schedule_interval_weekly = "0 1 * * SUN"
replicon_conn_id = 'seaspanshipyardsora_replicon_rnadmin'
