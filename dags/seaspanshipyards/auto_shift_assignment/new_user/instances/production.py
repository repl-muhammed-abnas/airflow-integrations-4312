# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_shift_assignment.new_user.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'seaspanshipyards'
user_shift_report_name = "**Shift Assignment Report**"
reference_filepath =  '/Shiftautomation/Newusershiftassignment/Prod/referencefile/Prod_referenceusers.csv'

schedule_interval = "0 19 * * *"
replicon_conn_id = 'seaspanshipyards-replicon-admin'
seaspanshipyards_sftp_conn_id = 'Seaspan_internal Sftp'
