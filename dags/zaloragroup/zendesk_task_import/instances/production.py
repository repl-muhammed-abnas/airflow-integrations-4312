# pylint: disable=wildcard-import unused-wildcard-import
from zaloragroup.zendesk_task_import.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'

company_key = 'zaloragroup'
replicon_conn_id = 'zaloragroup_replicon_admin'

input_filepath = '/Zendesk_Task Import'
archive_filepath = '/Zendesk_Task Import/Archive'
log_filepath = '/Zendesk_Task Import/Logs'

sftp_conn_id = 'sftp_zaloragroup_636673'

tenant_email = 'frank.dadural@zalora.com,bhavya.sethi@zalora.com,kimberly.bernarte@my.zalora.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
