# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_us_v2.config import *
from crl.time_export_us_v2.mapper.paycode_mapper import PAY_CODE_MAPPER
from crl.time_export_us_v2.mapper.employee_types_mapper import EMPLOYEE_TYPE_MAPPER, PROJECT_EMPLOYEE_TYPE_MAPPER

pay_code_mapper = PAY_CODE_MAPPER
non_project_employee_types = EMPLOYEE_TYPE_MAPPER
project_employee_types = PROJECT_EMPLOYEE_TYPE_MAPPER

instance = "uat"

company_key = "CharlesRiverLaboratoriesSandbox"

replicon_conn_id = "charlesriverlaboratoriessandbox_replicon_rit"
sftp_conn_id = "sftp_charlesriverlaboratoriessandbox_603355"
http_conn_id = 'charlesriverlaboratoriessandbox_timedata_http_conn'
http_conn_id_dev = 'charlesriverlaboratoriessandbox_timedata_http_conn_dev'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

export_locations = "USA"

time_export_process_export_dag_id = f"crl_us_time_export_master_{instance}_v2"

timeexport_upload_input_filepath = "/Test/Outbound/Time Export"
logs_filepath = "/Test/Outbound/Time Export/Logs"
