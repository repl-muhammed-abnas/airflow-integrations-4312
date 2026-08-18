# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.workday_user_sync.time_off_plan.config import *

instance = "trial"
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
input_filepath = "/Workday/Time off Plan/Sandbox/Input"
archive_filepath = "/Workday/Time off Plan/Sandbox/Archive"
log_filepath = "/Workday/Time off Plan/Sandbox/Log"
disabled = True
