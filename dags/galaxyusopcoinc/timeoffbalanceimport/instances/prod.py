# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.timeoffbalanceimport.config import *

instance = "production"
environment = 'production'

company_key = 'GalaxyUSOpcoInc'

replicon_conn_id = 'galaxyusopcoinc_replicon_admin'
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'

pgp_conn_id = "pgp_vialto_partners"

input_filepath = '/Workday/Time Off Balance/Prod/Input'
archive_filepath = '/Workday/Time Off Balance/Prod/Archive'
log_filepath = '/Workday/Time Off Balance/Prod/Log'
reference_file = "/Workday/Time Off Balance/Prod/Reference/timeoff_balance_reference_file.csv"

tenant_email = 'gbl_vialto_technology_digital_replicon_time_entry@vialto.com,utpal.chakraborty@vialto.com,hemanth.maru@vialto.com,farhan.afzal@vialto.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name= f'vialtopartners_timeoffbalance_importrun_batch_task_{instance}'
