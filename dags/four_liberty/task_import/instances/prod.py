# pylint: disable=wildcard-import unused-wildcard-import
from four_liberty.task_import.config import *

region = "us-east-1"
environment = "production"

instance = 'prod'
company_key = "4Liberty"

sftp_conn_id = "4liberty_sftp_668049"
sftp_conn_id2 = "4liberty_sftp_Integration_uswest"
replicon_conn_id = "4liberty-replicon-dataintegration"

input_filepath = "/Production/input"
processing_filepath = "/Production/processing"
archive_filepath = "/Production/input/archive"
reference_filepath = "/4liberty/reference"
reference_archive_filepath = "/4liberty/reference/archive"
log_filepath = "/Production/logs"

tenant_email = "4L.TEAdmin@4liberty.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

can_run_batch_task_var_name = f'four_liberty_task_import_{instance}_can_run_batch_task'
