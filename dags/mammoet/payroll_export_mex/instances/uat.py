# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.payroll_export_mex.config import *
from mammoet.payroll_export_mex.mappers.paycodes_mex import MX_PAYCODES


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
can_run_batch_task_var_name = f"mammoet_payroll_export_mex_can_run_batch_task_var_{instance}"

PAYCODES = MX_PAYCODES
PAYROLL_LOCATION_NAME = "Mexico"
PAYROLL_FILE_PREFIX = "MX_"

LOCATION_CODE = "MEX"

dag_instance_postfix = f"{LOCATION_CODE}_{instance}"

payroll_export_daily_master_dag_id = f"mammoet_payroll_export_daily_master_{dag_instance_postfix}"
payroll_export_process_payroll = f"mammoet_payroll_export_process_payroll_child_{dag_instance_postfix}"
payroll_export_post_export_dag_id = f"mammoet_payroll_export_post_data_to_api_child_{dag_instance_postfix}"
payroll_export_monthly_master_dag_id = f"mammoet_payroll_export_monthly_master_{dag_instance_postfix}"

# ONLY APPLICABLE FOR UAT
# Sandbox: run every 2 hours
daily_run_schedule_interval = "30 */2 * * *"
