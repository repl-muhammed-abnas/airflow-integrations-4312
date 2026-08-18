# pylint: disable=wildcard-import unused-wildcard-import
from lanter_delivery_systems.report_to_sftp.config import *

company_key = "ldstrial01"
instance = "trial"
environment = 'pre-production'

sftp_conn_id = "ldstrial01_sftp_replicon"

replicon_conn_id = "ldstrial01_replicon_admin"
log_filepath = "/uploads/"

schedule_interval = "0 13 * * 1"

file_name = "Labor_Report_for_VNDLY_with_PayCodes_Jacob"


report_name = "Labor Report for VNDLY with PayCodes - Jacob"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

disable=True

disabled=True
