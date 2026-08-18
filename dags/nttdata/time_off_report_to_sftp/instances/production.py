# pylint: disable=wildcard-import unused-wildcard-import
from nttdata.time_off_report_to_sftp.config import *

instance = 'production'
environment = 'production'

company_key = 'nttdata'
master_dag_max_active_runs = 1
report_name = "**NTTData New TimeOff Extract- 2020"

replicon_conn_id = 'nttdata_replicon_replicon'
sftp_conn_id = 'nttdata_sftp_NTT_ClarityPPM'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
