# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.payroll_export_bel.config import *
from mammoet.payroll_export_bel.mappers.paycodes_bel import BE_PAYCODES


instance = "prod"
environment = "production"

company_key = "mammoet"

replicon_conn_id = "mammoet_replicon_admin"
sftp_conn_id = "sftp_mammoet_550793"
http_conn_id = f'mammoet_payroll_export_be_http_conn_{instance}'

tenant_email = 'RepliconNotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

payroll_export_daily_master_dag_id = f"mammoet_payroll_export_daily_master_BE_{instance}"
payroll_export_process_payroll = f"mammoet_payroll_export_process_payroll_child_BE_{instance}"
payroll_export_post_export_dag_id = f"mammoet_payroll_export_post_data_to_api_child_BE_{instance}"
payroll_export_monthly_master_dag_id = f"mammoet_payroll_export_monthly_master_BE_{instance}"

payroll_export_upload_input_filepath = "/Production/Payroll Export/Input"
payroll_export_upload_backup_filepath = "/Production/Payroll Export/Backup"

client_id_secret_variable_name = f"mammoet_client_id_secret_variable_{instance}"

PAYCODES = BE_PAYCODES
PAYROLL_LOCATION_NAME = "Belgium"
PAYROLL_FILE_PREFIX = "BE_"

daily_run_schedule_interval = "0 11 2-28,29,30,31 * *"
monthly_run_schedule_interval = "0 11 1 * *"
