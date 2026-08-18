# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.payroll_export_nld.config import *
from mammoet.payroll_export_nld.mappers.paycodes_nld_v1 import NL_PAYCODES


instance = "prod"
environment = "production"

company_key = "mammoet"

replicon_conn_id = "mammoet_replicon_admin"
sftp_conn_id = "sftp_mammoet_550793"
http_conn_id = f'mammoet_payroll_export_be_http_conn_{instance}'

tenant_email = 'RepliconNotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

payroll_export_upload_input_filepath = "/Production/Payroll Export/Input"
payroll_export_upload_backup_filepath = "/Production/Payroll Export/Backup"

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


# ONLY APPLICABLE FOR PROD
time_zone = "CET"
daily_run_schedule_interval = "0 12 1,3-28,29,30,31 * *"
monthly_run_schedule_interval = "30 12 2 * *"
