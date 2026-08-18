from sandtechinc.user_import.config import *

environment = 'pre-production'
instance = "uat"
company_key = 'sandtechinctrial01'
replicon_conn_id = 'sandtechinctrial01_replicon_admin'
can_run_batch_task_var_name = f'sandtechinc_user_import_can_run_batch_task_{instance}'

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
sftp_conn_id = "sftp_sandtechinc_696582"
input_filepath = "/Trial/UserImport/Input"
archive_filepath = "/Trial/UserImport/Archive"
reference_filepath = "/Trial/UserImport/Reference"
log_filepath = "/Trial/UserImport/Logs"

# Email Configuration
tenant_email = "mhilburn@sandtech.com,jratcliffe@sandtech.com,ovanwyk@sandtech.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'