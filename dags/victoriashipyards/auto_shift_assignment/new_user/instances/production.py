# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.auto_shift_assignment.new_user.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'VictoriaShipyards'
user_shift_report_name = "**Shift Assignment Report**"
reference_filepath =  '/Shiftautomation/Newusershiftassignment/VictoriaShipyards/referencefile/prod_referenceusers.csv'

schedule_interval = "0 19 * * *"
replicon_conn_id = 'VictoriaShipyards-replicon-repliconint'
victoriashipyards_sftp_conn_id = 'victoriashipyards_sftp_admin'
