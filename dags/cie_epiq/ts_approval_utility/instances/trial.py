# pylint: disable=wildcard-import unused-wildcard-import
from cie_epiq.ts_approval_utility.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'
company_key = 'epiqsystemsinctrial01'
replicon_conn_id = "replicon_epiq_trial"

tenant_email = "PradipKumar@deltek.com,RahulGajeli@deltek.com"
internal_logs_email = "PradipKumar@deltek.com,RahulGajeli@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bucket_name = "replicon-airflow-dev-cie-group"

max_master_run = 1
max_child_run = 5
chunk_size = 200
schedule_interval = "00 8 * * MON"
timezone = "America/New_York"

#disabled = True
