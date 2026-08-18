# pylint: disable=wildcard-import unused-wildcard-import
from solaris.report_to_sftp.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'SolarisMCI'
replicon_conn_id = 'solarismci_replicon_newadmin'
sftp_conn_id = 'solarismci_sftp_replicon'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
