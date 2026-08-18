# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.timeoff_import.config import *

instance = "production"
environment = 'production'

company_key = 'GalaxyUSOpcoInc'

replicon_conn_id = 'galaxyusopcoinc_replicon_admin'
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'

pgp_conn_id = "pgp_vialto_partners"

input_filepath = '/Workday/LOA & RFL/Prod/Input'
archive_filepath = '/Workday/LOA & RFL/Prod/Archive'
log_filepath = '/Workday/LOA & RFL/Prod/Log'

tenant_email = 'gbl_vialto_technology_digital_replicon_time_entry@vialto.com,utpal.chakraborty@vialto.com,hemanth.maru@vialto.com,farhan.afzal@vialto.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'vialtopartners_timeoff_import_run_batch_task_{instance}'
can_decrypt_file = f'vialtopartners_timeoff_import_can_decrypt_file_{instance}'
