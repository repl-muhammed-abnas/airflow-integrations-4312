# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_shift_assignment.new_user.config import *

instance = 'sandbox2'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'SeaspanShipyardsOra'
user_shift_report_name = "**Shift Assignment Report**"
reference_filepath =  '/seaspanshipyardora/Shiftautomation/Newusershiftassignment/Prod_referenceusers.csv'

schedule_interval = "0 19 * * *"
replicon_conn_id = 'seaspanshipyardsora_replicon_rnadmin'
seaspanshipyards_sftp_conn_id = 'Seaspanshipyardssb_Integration_uswest'
