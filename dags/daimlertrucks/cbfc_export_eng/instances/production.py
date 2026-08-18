# pylint: disable=wildcard-import unused-wildcard-import
from daimlertrucks.cbfc_export_eng.config import *

instance = 'production'
environment = 'production'
company_key = 'daimlertrucks'
replicon_conn_id = 'daimlertrucks_replicon_replicon'

tenant_email = "Replicon-Support@daimlertruck.com"
internal_email = '{{ var.value.dagrun_internal_log_email }}'


execution_timeout_days = 1
master_dag_schedule_interval = "5 8 * * *"
sftp_conn_id = "DaimlerTrucks_sftp"
report1_name = "***CbFC_Export_ENG****"
sftp_filepath = "/Production/CbFCExport/DTNA_ENG/Outbound"
sftp_Archive_filepath = "/Production/CbFCExport/DTNA_ENG/Archives"
