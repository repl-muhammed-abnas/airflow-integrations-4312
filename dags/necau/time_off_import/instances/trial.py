# pylint: disable=wildcard-import unused-wildcard-import
from necau.time_off_import.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'
sftp_conn_id = "necauafmig_replicon_sftp"
company_key = 'NECAUafmig'
replicon_conn_id = 'NECAUafmig_replicon_admin'
user_shift_report_name = "***Auto Shift Assignment-Master***"
timeoff_import_user_referance = "***Timeoff Import User Reference"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
can_run_batch_task_var_name = f'nec_timeoff_import_{instance}_can_run_batch_task'
disabled = True
