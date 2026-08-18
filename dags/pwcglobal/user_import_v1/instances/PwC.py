# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import_v1.config import *

instance = 'PwC'
region = 'eu-central-1'
environment = 'production'

company_key = 'PwC'
replicon_conn_id = 'pwcglobal-replicon-eu.userimport'
sftp_conn_id = "pwcglobal-MFT-PRD-replicon"

input_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/PMDM"
archive_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/PMDM/_archive"
log_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/PMDM/_logs"

tenant_email = 'PWCGlobalLogs@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

is_secondary_upload_required = False

user_dag_max_active_runs = 10
supervisor_dag_max_active_runs = 10
