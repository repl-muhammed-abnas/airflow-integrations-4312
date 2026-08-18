# pylint: disable=wildcard-import unused-wildcard-import
from odessa.resource_allocation_removal_on_holidays.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'Odessaafmig'

replicon_conn_id = 'odessaafmig_replicon_admin'
user_data_report_name = 'UserData_forallocation'

log_file_path = 'Odessaafmig/removeresourceallocation/Logs/'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
