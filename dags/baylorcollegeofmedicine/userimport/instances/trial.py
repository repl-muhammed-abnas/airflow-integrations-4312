# pylint: disable=wildcard-import unused-wildcard-import
from baylorcollegeofmedicine.userimport.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'BaylorCollegeofmedicineafmig'
replicon_conn_id = 'baylorcollegeofmedicine_replicon_repadmin'
sftp_conn_id = "sftp_useast2"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

max_active_runs_group = 5
max_active_runs_user = 10

input_filepath = '/Baylorcollegeofmedicine/Input'
reference_filepath = '/Baylorcollegeofmedicine/Reference/'
archive_filepath = '/Baylorcollegeofmedicine/Archive/'
log_filepath = '/Baylorcollegeofmedicine/Logs/'


can_run_batch_task = f'baylorcollegeofmedicine_user_import_can_run_batch_task_{instance}'

disabled=True
