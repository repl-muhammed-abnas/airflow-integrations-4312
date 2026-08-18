# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import.config import *

instance = 'PwCQA'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCQA'
replicon_conn_id = 'pwcqa-replicon-eu.userimport'
sftp_conn_id = "pwcglobalqa-MFT-STG-replicon"

input_filepath = "/PwCGBL_RepliconGlobal_STG/PeopleData/Inbound"
archive_filepath = "/PwCGBL_RepliconGlobal_STG/PeopleData/Archive"
log_filepath = "/PwCGBL_BOSALLogs_STG/QA/ToPwC"

tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
