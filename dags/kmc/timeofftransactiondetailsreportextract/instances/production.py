# pylint: disable=wildcard-import unused-wildcard-import
from kmc.timeofftransactiondetailsreportextract.config import *

instance = 'production'
environment = 'production'

company_key = '10272kmc'
master_dag_max_active_runs = 1
report_name = "Time Off Transaction Details"

replicon_conn_id = '10272kmc_replicon_admin'
sftp_conn_id = '10272kmc_sftp_626047'

tenant_email = "HR@rpmmachinery.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
