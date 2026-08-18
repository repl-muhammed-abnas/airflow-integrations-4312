# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_shift_assignment.new_user.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'seaspanshipyardsafmig'
user_shift_report_name = "**Shift Assignment Report**"
reference_filepath =  '/Shiftautomation/Newusershiftassignment/Prod/referencefile/Prod_referenceusers.csv'

schedule_interval = "0 19 * * *"
replicon_conn_id = 'seaspanshipyardsafmig_replicon_admin'
seaspanshipyards_sftp_conn_id = 'seaspanshipyardsafmig_sftp_admin'
disabled = True
