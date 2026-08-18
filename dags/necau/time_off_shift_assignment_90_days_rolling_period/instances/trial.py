# pylint: disable=wildcard-import unused-wildcard-import
from necau.time_off_shift_assignment_90_days_rolling_period.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'
sftp_conn_id = "necauafmig_replicon_sftp"
company_key = 'NECAUafmig'
replicon_conn_id = 'NECAUafmig_replicon_admin'
user_shift_report_name = "***Shift Assignment Time Off User reference_1***"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
dag_max_active_runs = 1

disabled=True
