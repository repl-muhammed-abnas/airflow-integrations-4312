# pylint: disable=wildcard-import unused-wildcard-import
from moodys.timeoff_import_v1.config import *

instance = "production"
environment = "production"
version = "v1"

company_key = "MoodysEMEA"

replicon_conn_id = "moodysemea_replicon_integrationuser"
sftp_conn_id = "sftp_moodysemea_654601"
pgp_conn_id = "pgp_moodysemea_timeoffsync"

input_filepath = "/MoodysEMEA/Prod/Timeoffsync/Input"
archive_filepath = "/MoodysEMEA/Prod/Timeoffsync/Archive"
log_filepath = "/MoodysEMEA/Prod/Timeoffsync/Logs"

tenant_email = "chanel.benjamin@moodys.com,globalpayrollintegration@moodys.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f'moodys_timeoff_import_master_{instance}_{version}'
child_dag_id = f'moodys_timeoff_import_process_each_record_child_{instance}_{version}'

can_run_batch_task_name = f'moodys_timeoff_import_batch_run_{instance}_{version}'
