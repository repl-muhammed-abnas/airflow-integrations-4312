# pylint: disable=wildcard-import unused-wildcard-import
from isuzu.invoice_sync.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'isuzuafmig'
replicon_conn_id = 'isuzuafmig_replicon_admin'
http_conn_id = "replicon_service_isuzuafmig_trial_invoice_creation"
sftp_conn_id = 'client_horizon_sftp'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

schedule_interval = 300
max_active_runs = 5
execution_timeout = 14

input_filepath = "/Production/Input"
archive_filepath = "/Production/Archives"

report1_name = "Invoice data for Integration"
report2_name = "Invoiced amount (Project Wise)"

can_run_batch_task_var_name = f'can_run_{instance}_{company_key}_invoice_sync'
token_var = f"{company_key}_{instance}_replicon_service_token"
disabled = True
