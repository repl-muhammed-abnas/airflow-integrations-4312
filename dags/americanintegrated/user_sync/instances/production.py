# pylint: disable=wildcard-import unused-wildcard-import
from americanintegrated.user_sync.config import *

environment = 'production'
instance = "production"

company_key = 'AmericanIntegrated'
replicon_conn_id = 'americanintegrated_user_import_admin'
sftp_conn_id = "sftp_uswest_647462"

input_filepath = "/user/input"
archive_filepath = "/user/Archive"
referance_filepath = "/user/reference"
log_filepath = "/user/logs"

tenant_email = 'andelgado@americanintegrated.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

pacific_timezone = 'US/Pacific'

user_import_master = f'american_integrated_user_import_master_{instance}'
user_import_update_child = f'american_integrated_user_update_child_{instance}'
user_import_add_child = f'american_integrated_user_add_child_{instance}'

can_run_batch_task_var_name = f'americanintegrated_user_import_can_run_batch_task_{instance}'
