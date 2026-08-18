# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.payroll_export_spain.config import *
from mammoet.payroll_export_spain.mappers.paycodes_spain_v1 import SPAIN_PAYCODES


instance = "uat"

company_key = "mammoettrial01"

replicon_conn_id = "mammoettrial01_replicon_admin"
sftp_conn_id = "sftp_mammoet_uat"
http_conn_id = f'mammoettrial01_time_payroll_export_http_conn_{instance}'

tenant_email = 'repliconnotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

payroll_export_upload_input_filepath = "/Payroll Export/Trial01/Input"
payroll_export_upload_backup_filepath = "/Payroll Export/Trial01/Backup"

client_id_secret_variable_name = f"mammoet_client_id_secret_variable_{instance}"

PAYCODES = SPAIN_PAYCODES
PAYROLL_LOCATION_NAME = "Spain"
PAYROLL_FILE_PREFIX = "ES_"

LOCATION_CODE = "ES"

dag_instance_postfix = f"{LOCATION_CODE}_{instance}"

payroll_export_daily_master_dag_id = f"mammoet_payroll_export_daily_master_{dag_instance_postfix}"
payroll_export_process_payroll = f"mammoet_payroll_export_process_payroll_child_{dag_instance_postfix}"
payroll_export_post_export_dag_id = f"mammoet_payroll_export_post_data_to_api_child_{dag_instance_postfix}"
payroll_export_monthly_master_dag_id = f"mammoet_payroll_export_monthly_master_{dag_instance_postfix}"

# ONLY APPLICABLE FOR UAT
daily_run_schedule_interval = "30 */2 * * *"

