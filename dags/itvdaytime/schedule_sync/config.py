region = 'eu-central-1'
environment = 'pre-production'

company_key = "itvdaytimetrial01"

sftp_conn_id = "sftp_useast2"  # "sftp-itvdaytime-internal"

replicon_conn_id = "replicon-itvdaytime-radmin"

upload_filepath = "iTV/Trial/Export/Schedule"

master_dag_schedule = "0 1,13 * * *"

max_active_runs_master = 1
child_max_active_runs = 10

schedule_sync_report_name = "***Work Schedule Details Report"

# to be updated as per spec while deploying for UAT
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_internal_testing_email }}'
