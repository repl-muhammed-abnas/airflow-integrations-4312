# pylint: disable=wildcard-import unused-wildcard-import
from b2g.project_billing_rate_assignment_removal.config import *

region = 'eu-central-1'
instance = "production"
environment = 'production'
company_key = 'WRDT'

replicon_conn_id = 'wrdt_replicon_admin'
sftp_conn_id = "sftp_Integration_GmailtoSFTP"

input_filepath = '/B2G/b2g.billingratesync/Input/'
new_filepath = '/B2G/b2g.billingratesync/fromaddress/'
archive_filepath = '/B2G/b2g.billingratesync/Archive/'

can_run_batch_task_child = f'project_billing_rate_{instance}_can_run_batch_task'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
