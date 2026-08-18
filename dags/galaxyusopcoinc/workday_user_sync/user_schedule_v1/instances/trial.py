# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.workday_user_sync.user_schedule_v1.config import *

instance = "trial"
sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = "/Workday/Work Schedules/Test/Input"
archive_filepath = "/Workday/Work Schedules/Test/Archive"
log_filepath = "/Workday/Work Schedules/Test/Log"

dag_id_postfix = f'{instance}_v1'
disabled = True
