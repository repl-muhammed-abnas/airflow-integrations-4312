# pylint: disable=wildcard-import unused-wildcard-import
from baylorcollegeofmedicine.userimport.config import *

instance = "production"
environment = 'production'
company_key = 'BaylorCollegeofmedicine'
replicon_conn_id = 'baylorcollegeofmedicine_replicon_bimport'
sftp_conn_id = "sftp_baylorcollegeofmedicine_528574"

tenant_email = "replicon@bcm.edu"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

max_active_runs_group = 5
max_active_runs_user = 10

input_filepath = '/Gen3_UserImport/Input'
reference_filepath = '/Gen3_UserImport/Reference/'
archive_filepath = '/Gen3_UserImport/Archive/'
log_filepath = '/Gen3_UserImport/Logs/'


can_run_batch_task = f'baylorcollegeofmedicine_user_import_can_run_batch_task_{instance}'
