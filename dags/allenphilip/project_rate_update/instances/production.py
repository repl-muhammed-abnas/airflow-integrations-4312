# pylint: disable=wildcard-import unused-wildcard-import
from allenphilip.project_rate_update.config import *

region = 'us-east-1'
instance = "production"
environment = 'production'
company_key = 'AllenPhilp'

replicon_conn_id = 'AllenPhilp_replicon_Dortis'
sftp_conn_id = "sftp_gmailToSFTP_Integration_GmailtoSFTP"

input_filepath = '/Allenphilp/allenphilp.projectrateupdate/Input'
log_file_path = 'AllenPhilp/ProjectBillingRateUpdate/'
archieve_filepath = '/Allenphilp/allenphilp.projectrateupdate/Archive/'
address_filepath = '/Allenphilp/allenphilp.projectrateupdate/fromaddress/'

can_run_batch_task_child = f'allenphilip_project_rate_update_child_{instance}_can_run_batch_task'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
