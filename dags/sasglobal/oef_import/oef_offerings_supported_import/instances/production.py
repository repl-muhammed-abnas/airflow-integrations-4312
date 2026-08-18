# pylint: disable=wildcard-import unused-wildcard-import
from sasglobal.oef_import.oef_offerings_supported_import.config import *

instance = 'production'
environment = 'production'

company_key = 'sasglobalprod'

replicon_conn_id = 'SASGlobalProd_replicon_replicon'
sftp_conn_id = 'sasglobal_sftp_568340'
pgp_conn_id = 'pgp_SASGlobalProd_oefimport'

input_filepath = "/Inbound/OEF/Offerings Supported/processing"
archive_filepath = "/Inbound/OEF/archive"
log_filepath = "/Inbound/OEF/Offerings Supported/Logs"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
