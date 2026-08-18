# pylint: disable=wildcard-import unused-wildcard-import
from frontdoorinc.user_import.config import *

instance = "production"
environment = 'production'
company_key = 'FrontdoorInc'

replicon_conn_id = 'frontdoorinc_replicon_admin'
sftp_conn_id = "sftp_frontdoor_7ucznjqm55nt"
workday_http_conn_id = 'frontdoorinc_user_import_workday_http_connection'

log_filepath = '/usersync/userimportlogs'
reference_file_path = '/usersync/reference/'
archive_filepath = '/usersync/archive/'
input_filepath = '/usersync/input/'

can_run_batch_task_var_name = f'frontdoorinc_user_import_child_{instance}_can_run_batch_task'

tenant_email = "Bryce.DeBruce@frontdoorhome.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
