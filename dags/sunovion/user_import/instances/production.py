# pylint: disable=wildcard-import unused-wildcard-import
from sunovion.user_import.config import *

instance = "production"
environment = 'production'
company_key = 'Sunovion'
replicon_conn_id = 'sunovion_replicon_admin'
sftp_conn_id = "sftp_sunovion_557911_workato_useast"

tenant_email = "Chris.Johnson@sunovion.com,hrsystems@sunovion.com,Julie.Chaves@sunovion.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_support_email_cc = "sunovionsupport@replicon.com"

input_filepath = '/557911PDWD/Processing/Processing'
archive_filepath = '/557911PDWD/Processing/Archive/'
log_filepath = '/557911PDWD/Processing/Logs/'


can_run_batch_task = f'sunovion_user_import_can_run_batch_task_{instance}'
