# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_us_v1.config import *
from crl.time_export_us_v1.mapper.paycode_mapper import PAY_CODE_MAPPER

pay_code_mapper = PAY_CODE_MAPPER

instance = "prod"

region = 'us-east-1'
environment = "production"

company_key = "CharlesRiverLaboratories"

replicon_conn_id = "CharlesRiverLaboratories_repliconint_timeexport"
sftp_conn_id = 'sftp_charlesriverlaboratories_603355'
http_conn_id = 'charlesriverlaboratories_timedata_http_conn'
http_conn_id_dev = 'charlesriverlaboratories_timedata_http_conn_dev'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

export_locations = "USA"

time_export_master_dag_id = f"crl_us_time_export_master_{instance}_v1"
time_export_process_export_dag_id = f"crl_us_time_export_process_time_export_child_{instance}_v1"

timeexport_upload_input_filepath = "/Production/Outbound/Time Export"
logs_filepath = "/Production/Outbound/Time Export/Logs"
