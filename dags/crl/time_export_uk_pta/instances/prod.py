# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_uk_pta.config import *
from crl.time_export_uk_pta.mapper.paycode_mapper import UK_PAY_CODE_MAPPER

pay_code_mapper = UK_PAY_CODE_MAPPER

instance = "prod"
environment = "production"

company_key = "CharlesRiverLaboratories"

replicon_conn_id = "CharlesRiverLaboratories_repliconint_timeexport"
sftp_conn_id = 'sftp_charlesriverlaboratories_603355'
http_conn_id = 'charlesriverlaboratories_timedata_http_conn'
http_conn_id_dev = 'charlesriverlaboratories_timedata_http_conn_dev'


dag_active = True

# DAG IDs
time_export_master_dag_id = f"crl_time_export_uk_pta_master_{instance}"
time_export_process_export_dag_id = f"crl_time_export_uk_pta_process_child_{instance}"

# Location filter for UK
export_locations = "GBR"

# Email configuration
tenant_email = 'SAPCPISUPPORT@charlesriverlabs.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,Prabhav.Potluri@crl.com,Sean.Cotto@crl.com,lakshmanarao.mandala@crl.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# SFTP paths
timeexport_upload_input_filepath = "/Production/Outbound/Time Export"
logs_filepath = "/Production/Outbound/Time Export/Logs"