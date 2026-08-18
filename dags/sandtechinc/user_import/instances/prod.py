from sandtechinc.user_import.config import *


instance = "prod"
environment = 'production'

company_key = 'sandtechinc'

# Connection IDs
replicon_conn_id = 'sandtechinc_replicon_admin'
sftp_conn_id = "sftp_sandtechinc_696582"

# DAG IDs
main_dagid = f"sandtechinc_user_import_master_{instance}"
add_user_child_dagid = f"sandtechinc_user_import_add_child_{instance}"
update_user_child_dagid = f"sandtechinc_user_import_update_child_{instance}"
supervisor_child_dagid = f"sandtechinc_user_import_supervisor_child_{instance}"
create_role_child_dagid = f"sandtechinc_user_import_role_child_{instance}"

# Schedule: Daily at 1:00 AM EST
est_timezone = 'America/New_York'
schedule_interval_daily = '0 1 * * *'

# SFTP Configuration
input_filepath = "/Production/UserImport/Input"
archive_filepath = "/Production/UserImport/Archive"
reference_filepath = "/Production/UserImport/Reference"
log_filepath = "/Production/UserImport/Logs"

# Email Configuration
tenant_email = "mhilburn@sandtech.com,jratcliffe@sandtech.com,ovanwyk@sandtech.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'sandtechinc_user_import_can_run_batch_task_{instance}'
