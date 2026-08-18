# pylint: disable=wildcard-import unused-wildcard-import
from lanter_delivery_systems.report_to_sftp.config import *

company_key = "lds"
instance = "production"
environment = 'production'

sftp_conn_id = "client_sftp_lds_replicon"

replicon_conn_id = "lds_replicon_admin"
log_filepath = "/uploads/"

schedule_interval = "0 13 * * 1"

file_name = "Labor_Report_for_VNDLY_with_PayCodes_Jacob"


report_name = "Labor Report for VNDLY with PayCodes - Jacob"

tenant_email = "Jacob.Grass@rubinbrown.com,AWelden@lanterds.com,rstone@ctr.lanterds.com,hr@lanterds.com,recruiting@lanterds.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
