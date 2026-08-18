# pylint: disable=wildcard-import unused-wildcard-import
from kmhavintegrationinc.adhoc.add_entry_tenant_wide_logs.config import *

region = 'us-east-1'
instance = "production"
environment = 'production'

company_key = 'KMHAVIntegrationInc'

replicon_conn_id = 'KMHAVIntegrationInc_replicon_khenneman'
sftp_conn_id = 'sftp_useast2'

input_filepath = '/Adhoc/kmhav/prod/Input'
archive_filepath = '/Adhoc/kmhav/prod/Archive'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled = True
