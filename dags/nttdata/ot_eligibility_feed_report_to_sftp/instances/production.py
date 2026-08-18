# pylint: disable=wildcard-import unused-wildcard-import
from nttdata.ot_eligibility_feed_report_to_sftp.config import *

instance = 'production'
environment = 'production'

company_key = 'nttdata'
master_dag_max_active_runs = 1
report_name = "OT Eligibility Feed"

replicon_conn_id = 'nttdata_replicon_replicon'
sftp_conn_id = 'nttdata_sftp_ntt_replicon'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
