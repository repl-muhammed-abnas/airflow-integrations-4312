# pylint: disable=wildcard-import unused-wildcard-import
from abbviemst.time_extract.config import *

region = 'us-east-1'
environment = 'production'
instance = "production"
company_key = "AbbvieMST"

# Timezone configuration
time_zone = "US/Central"

sftp_conn_id = "sftp_abbviemst_510397"

replicon_conn_id = "abbviemst_replicon_radmin"

upload_filepath = "/RepliconTimeExtract/Prod"

tenant_email = "SnTBTS@abbvie.com,pritesh.olwe@replicon.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


master_dag_id = f'abbviemst_time_export_master_dag_{instance}'
time_extract_delta_child_dagid = f'abbviemst_time_extract_delta_child_{instance}'
