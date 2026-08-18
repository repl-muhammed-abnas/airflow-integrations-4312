# pylint: disable=wildcard-import unused-wildcard-import
from frontdoorinc.user_import.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'frontdoorincafmig'

replicon_conn_id = 'frontdoorincafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"
workday_http_conn_id = 'frontdoorincafmig_user_import_workday_http_connection'

log_filepath = '/usersync/userimportlogs'
reference_file_path = '/usersync/reference/'
archive_filepath = '/usersync/archive/'
input_filepath = '/usersync/input/'

can_run_batch_task_var_name = f'frontdoorinc_user_import_child_{instance}_can_run_batch_task'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
