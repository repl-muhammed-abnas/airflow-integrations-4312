# pylint: disable=wildcard-import unused-wildcard-import
from necau.time_off_import.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'
company_key = 'necau'
replicon_conn_id = 'necau-replicon-admin'
tenant_email = "HRISSupport@nec.com.au"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
sftp_conn_id = "NECProd_SFTP"
timeoff_import_file_directory = "/AU/FromFrontier"
processing_file_directory = "/AU/FromFrontier/processing"
unprocessed_file_directory = "/AU/FromFrontier/unprocessed"
archive_file_directory = "/AU/FromFrontier/Archived"
can_run_batch_task_var_name = f'nec_timeoff_import_{instance}_can_run_batch_task'
