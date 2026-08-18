# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_task_project_fields_file_merger.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = "sftp_useast2"

input_filepath = '/Trial/Inbound/gsapTask/Input'
processing_filepath = '/Trial/Inbound/gsapTask/Processing'
archive_filepath = '/Trial/Inbound/gsapTask/Archive'
log_filepath = '/Trial/Inbound/gsapTask/logs'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
