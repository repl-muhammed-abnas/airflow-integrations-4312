# pylint: disable=wildcard-import unused-wildcard-import
from abbviemst.time_extract.config import *

environment = 'pre-production'
instance = "trial"
company_key = "abbviemstafmig"

# Timezone configuration
time_zone = "US/Central"

sftp_conn_id = "sftp_useast2"

replicon_conn_id = "abbviemstafmig_replicon_radmin"

upload_filepath = "/AbbvieMSTTrial01/time_extract/Logs"

tenant_email = "AirflowIntegrationTesting@deltek.com"
internal_logs_email = "AirflowIntegrationTesting@deltek.com"
alert_email = "AirflowIntegrationTesting@deltek.com"


master_dag_id = f'abbviemst_time_export_master_dag_{instance}'
time_extract_delta_child_dagid = f'abbviemst_time_extract_delta_child_{instance}'

disabled=True
