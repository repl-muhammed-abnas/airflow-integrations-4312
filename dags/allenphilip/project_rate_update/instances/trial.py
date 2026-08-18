# pylint: disable=wildcard-import unused-wildcard-import
from allenphilip.project_rate_update.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'AllenPhilpafmig'

replicon_conn_id = 'allenphilipafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"

input_filepath = '/Allenphilp/allenphilp.projectrateupdate/Input'
log_file_path = 'AllenPhilpafmig/ProjectBillingRateUpdate/'
archieve_filepath = '/Allenphilp/allenphilp.projectrateupdate/Archive/'
address_filepath = '/Allenphilp/allenphilp.projectrateupdate/fromaddress/'

can_run_batch_task_child = f'allenphilip_project_rate_update_child_{instance}_can_run_batch_task'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
