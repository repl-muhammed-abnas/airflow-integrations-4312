# pylint: disable=wildcard-import unused-wildcard-import
from sasglobal.oef_import.oef_offerings_supported_import.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'sasglobalprodafmig'

replicon_conn_id = 'sasglobalprodafmig_replicon'
sftp_conn_id = 'rsftp-useast_for_testing'
pgp_conn_id = 'pgp_sasglobal_oef_geo_import_trial'

input_filepath = "/SaSGlobalProdTrial/Inbound/OEF/Offerings Supported/processing"
archive_filepath = "/SaSGlobalProdTrial/Inbound/OEF/archive"
log_filepath = "/SaSGlobalProdTrial/Inbound/OEF/Offerings Supported/Logs"

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
