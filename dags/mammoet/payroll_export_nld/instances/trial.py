# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.payroll_export_nld.config import *
from mammoet.payroll_export_nld.mappers.paycodes_nld_v1 import NL_PAYCODES


instance = "trial"

company_key = "mammoettrial01trial01"

replicon_conn_id = "mammoettrial01trial01_replicon_admin"
sftp_conn_id = "sftp_useast2"
http_conn_id = f'mammoettrial01trial01_time_payroll_export_http_conn_{instance}'
# below connection Id's needs to be removed in UAT file
http_conn_id2 = f'mammoettrial01trial01_time_payroll_export_http_conn_{instance}_internal'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

payroll_export_upload_input_filepath = "/Payroll Export/Trial01Trial01/Input"
payroll_export_upload_backup_filepath = "/Payroll Export/Trial01Trial01/Backup"

client_id_secret_variable_name = f"mammoet_client_id_secret_variable_{instance}"

PAYCODES = NL_PAYCODES
PAYROLL_LOCATION_NAME = "Netherlands"
PAYROLL_FILE_PREFIX = "NL_"

LOCATION_CODE = "NLD"

dag_instance_postfix = f"{LOCATION_CODE}_{instance}"

payroll_export_daily_master_dag_id = f"mammoet_payroll_export_daily_master_{dag_instance_postfix}"
payroll_export_process_payroll = f"mammoet_payroll_export_process_payroll_child_{dag_instance_postfix}"
payroll_export_post_export_dag_id = f"mammoet_payroll_export_post_data_to_api_child_{dag_instance_postfix}"
payroll_export_monthly_master_dag_id = f"mammoet_payroll_export_monthly_master_{dag_instance_postfix}"

disable=True

disabled=True
