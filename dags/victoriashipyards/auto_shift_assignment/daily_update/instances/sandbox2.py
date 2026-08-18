# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.auto_shift_assignment.daily_update.config import *

instance = 'sandbox2'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'SeaspanVSLOra'
user_shift_report_name = "**Shift Assignment Report**"

schedule_interval_weekly = "0 14 * * 1,2,3,4"
replicon_conn_id = 'seaspanvslora_replicon_repliconint'
shift_assignment_daily_child_dagid=f'victoriashipyards_default_shift_assignment_daily_update_V3.0_{instance}'
shift_assignment_daily_master_dagid=f'victoriashipyards_daily_shift_assignment_master_{instance}'
