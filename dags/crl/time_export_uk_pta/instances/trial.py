# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_uk.config import *
from crl.time_export_uk.mapper.paycode_mapper import UK_PAY_CODE_MAPPER

pay_code_mapper = UK_PAY_CODE_MAPPER

instance = "trial"

region = 'us-east-1'
environment = "pre-production"

company_key = "CharlesRiverLaboratoriestrial01"

replicon_conn_id = "CharlesRiverLaboratoriestrial01_repliconint_timeexport"
sftp_conn_id = 'sftp_charlesriverlaboratories_603355_trial'
http_conn_id = 'charlesriverlaboratoriestrial01_timedata_http_conn'

dag_active = True  # Disabled by default

# DAG IDs
time_export_master_dag_id = "crl_time_export_uk_pta_trial"
time_export_process_export_dag_id = "crl_time_export_uk_pta_process_trial"

# Location filter for UK
export_locations = "GBR"

# Email configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

# SFTP paths
timeexport_upload_input_filepath = "/inbound/uk_pta"
logs_filepath = "/logs/uk_pta"