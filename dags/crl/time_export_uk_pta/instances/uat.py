# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_uk.config import *
from crl.time_export_uk.mapper.paycode_mapper import UK_PAY_CODE_MAPPER

pay_code_mapper = UK_PAY_CODE_MAPPER

instance = "uat"

region = 'us-east-1'
environment = "pre-production"

company_key = "CharlesRiverLaboratoriesSandbox"

replicon_conn_id = "CharlesRiverLaboratoriesSandbox_repliconint_timeexport"
sftp_conn_id = 'sftp_charlesriverlaboratories_603355_sandbox'
http_conn_id = 'charlesriverlaboratoriessandbox_timedata_http_conn'

dag_active = True

# DAG IDs
time_export_master_dag_id = "crl_time_export_uk_pta_uat"
time_export_process_export_dag_id = "crl_time_export_uk_pta_process_uat"

# Location filter for UK
export_locations = "GBR"

# Email configuration
tenant_email = 'SAPCPISUPPORT@charlesriverlabs.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,Prabhav.Potluri@crl.com,Sean.Cotto@crl.com,lakshmanarao.mandala@crl.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# SFTP paths
timeexport_upload_input_filepath = "/Test/Outbound/Time Export"
logs_filepath = "/Test/Outbound/Time Export/Logs"