# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.auto_shift_assignment.weekly_update.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'VictoriaShipyards'
user_shift_report_name = "**Shift Assignment Report**"

schedule_interval_weekly = "0 1 * * SUN"
replicon_conn_id = 'VictoriaShipyards-replicon-repliconint'
