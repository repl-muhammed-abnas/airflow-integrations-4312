# pylint: disable=wildcard-import unused-wildcard-import
from daimlertrucks.custom_email_notification.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'DaimlerTrucksafmig'
replicon_conn_id = 'DaimlerTrucksafmig'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'


execution_timeout_days = 1
master_dag_schedule_interval = "0 18 * * Mon-Fri"
sftp_conn_id = "client_horizon_sftp"
report1_name = "***CbFC_Export_ENG****"
sftp_filepath = "/Time off Sync/Log Files/Production/CbFCExport/DTNA_ENG/Outbound"
sftp_Archive_filepath = "/Time off Sync/Log Files/Production/CbFCExport/DTNA_ENG/Archives"
disabled = True
