# pylint: disable=wildcard-import unused-wildcard-import
from isuzu.invoice_sync.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'isuzu'
replicon_conn_id = 'isuzu_replicon_prateek'
http_conn_id = "replicon_service_isuzu_invoice_creation"
sftp_conn_id = 'sftp_isuzu_603812'

tenant_email = "Jonathan.Ostella@isza.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
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
