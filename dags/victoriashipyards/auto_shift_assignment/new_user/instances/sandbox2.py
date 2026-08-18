# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.auto_shift_assignment.new_user.config import *

instance = 'sandbox2'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'SeaspanVSLOra'
user_shift_report_name = "**Shift Assignment Report**"
reference_filepath =  '/Shiftautomation/Newusershiftassignment/SeaspanVSLOra/referencefile/SB2_referenceusers.csv'

schedule_interval = "0 19 * * *"
replicon_conn_id = 'seaspanvslora_replicon_repliconint'
victoriashipyards_sftp_conn_id = 'victoriashipyards_sftp_admin'
