from seaspanshipyards.auto_shift_assignment.daily_update.config import *

instance = 'sandbox'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'Seaspanshipyardssb'
user_shift_report_name = "**Shift Assignment Report**"

schedule_interval_weekly = "0 14 * * 1,2,3,4"
replicon_conn_id = 'seaspanshipyardssb_replicon_rnadmin'