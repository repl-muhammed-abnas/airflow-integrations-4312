# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.auto_shift_assignment.existing_user_update.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'VictoriaShipyards'
user_shift_report_name = "**Shift Assignment Report**"

schedule_interval = "0 19 1 * *"
replicon_conn_id = 'VictoriaShipyards-replicon-repliconint'
