region = 'us-east-2'
environment = 'pre-production'

company_key = 'abc2'
replicon_conn_id = 'gurustrial'

sftp_conn_id = 'sftp_internal'

instance = "Australia"

schedule_interval = '0 1 27,28,29,30 * SUN'

master_dag_max_active_runs = 1

report_name = 'TimeEntrySubmission_For_All_Locations'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
