region = 'us-east-2'
environment = 'pre-production'
company_key = 'repliconmatlentrial01'
replicon_conn_id = 'repliconmatlentrial01_replicon_admin'
sftp_conn_id = 'sftp_useast2'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_interval = 30
max_active_runs_master = 1
max_active_runs_process_each_records = 20
max_active_runs_process_new_user = 20
max_active_runs_process_update_user = 20
max_active_runs_process_supervisor_check = 20
max_active_runs_process_time_off_policy_new_user = 20
max_active_runs_process_time_off_assignment_new_user = 20
max_active_runs_process_time_off_assignment_update_user = 20
max_active_runs_process_time_off_policy_update_rehire_user = 20
execution_timeout_days = 14
execution_timeout_hours = 12


timeofftemplate = 'Time Off'
timesheet_period_schedule = 'Weekly'
schedule_policy = '8 hours/day, Su, Sa off'
