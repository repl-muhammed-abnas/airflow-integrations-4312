# pylint: disable=wildcard-import unused-wildcard-import
from hunterdickinsonservices.extract_report_to_sftp.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'hunterdickinsonservices'
replicon_conn_id = 'hunterdickinsonservices_replicon_eshwarkataiah'
sftp_conn_id = 'hunterdickinsonservices_sftp_RepliconFTP'

tenant_email = '{{ var.value.dagrun_internal_log_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
