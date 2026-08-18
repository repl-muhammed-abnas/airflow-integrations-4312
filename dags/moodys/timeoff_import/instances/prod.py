# pylint: disable=wildcard-import unused-wildcard-import
from moodys.timeoff_import.config import *

instance = "production"
environment = "production"

company_key = "MoodysEMEA"

replicon_conn_id = "moodysemea_replicon_integrationuser"
sftp_conn_id = "sftp_moodysemea_654601"
pgp_conn_id = "pgp_moodysemea_timeoffsync"

input_filepath = "/MoodysEMEA/Prod/Timeoffsync/Input"
archive_filepath = "/MoodysEMEA/Prod/Timeoffsync/Archive"
log_filepath = "/MoodysEMEA/Prod/Timeoffsync/Logs"

# pylint: disable=line-too-long
tenant_email = "chanel.benjamin@moodys.com,globalpayrollintegration@moodys.com"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_name = f'moodys_timeoff_import_batch_run_{instance}'
