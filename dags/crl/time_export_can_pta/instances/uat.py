# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_can_pta.config import *

instance = "uat"

company_key = "CharlesRiverLaboratoriesSandbox"

replicon_conn_id = "charlesriverlaboratoriessandbox_replicon_rit"
sftp_conn_id = "sftp_charlesriverlaboratoriessandbox_603355"
http_conn_id = 'charlesriverlaboratoriessandbox_timedata_http_conn'
http_conn_id_dev = 'charlesriverlaboratoriessandbox_timedata_http_conn_dev'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,MTL-Payroll@crl.com,Shari.Guttman@crl.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

export_locations = "CAN"

time_export_master_dag_id = f"crl_pta_can_time_export_master_{instance}"
time_export_process_export_dag_id = f"crl_pta_can_time_export_process_time_export_child_{instance}"

timeexport_upload_input_filepath = "/Test/Outbound/Time Export"
logs_filepath = "/Test/Outbound/Time Export/Logs"

client_id_secret_variable_name = f"crl_client_id_secret_variable_{instance}"
crl_time_export_bearer_token_variable = "crl_time_export_bearer_token_variable_uat"

