# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_shift_assignment.existing_user_update.config import *

instance = 'sandbox'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'Seaspanshipyardssb'
user_shift_report_name = "**Shift Assignment Report**"

schedule_interval = "0 19 1 * *"
replicon_conn_id = 'seaspanshipyardssb_replicon_rnadmin'
