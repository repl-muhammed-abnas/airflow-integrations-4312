from sandtechinc.user_import.config import *

environment = 'pre-production'
instance = "trial"
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
est_timezone = 'US/Eastern'
schedule_interval_daily = '0 1 * * *'

# SFTP Configuration
sftp_conn_id = "rsftp-useast_for_testing"
input_filepath = "/sandtechinc/UserImport/Test"
archive_filepath = "/sandtechinc/UserImport/Test/Archive"
reference_filepath = "/sandtechinc/UserImport/Test/Reference"
log_filepath = "/sandtechinc/UserImport/Test/Logs"

# Email Configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled = True