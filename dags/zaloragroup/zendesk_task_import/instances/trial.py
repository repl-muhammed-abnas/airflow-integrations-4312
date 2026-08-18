# pylint: disable=wildcard-import unused-wildcard-import
from zaloragroup.zendesk_task_import.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'zaloragroupafmig'
replicon_conn_id = 'zaloragroupafmig_replicon_zrtest'

input_filepath = '/ZaloraGroupTrial/Zendesk_Task Import'
archive_filepath = '/ZaloraGroupTrial/Zendesk_Task Import/Archive'
log_filepath = '/ZaloraGroupTrial/Zendesk_Task Import/Logs'

sftp_conn_id = 'rsftp-useast_for_testing'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
disabled = True
