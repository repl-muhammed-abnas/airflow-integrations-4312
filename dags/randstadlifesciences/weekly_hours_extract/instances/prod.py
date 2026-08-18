# pylint: disable=wildcard-import unused-wildcard-import
from randstadlifesciences.weekly_hours_extract.config import *

instance = 'production'
environment = 'production'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

company_key = 'randstadlifesciences'
replicon_conn_id = 'randstadlifesciences-replicon-admin'

sftp_conn_id = 'sftp_randstadlifesciences_replsftp'
upload_filepath = '/home/export'
