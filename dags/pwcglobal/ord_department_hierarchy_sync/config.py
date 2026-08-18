region = 'eu-central-1'
environment = 'pre-production'

replicon_conn_id = 'pwcinternal-replicon-eu.automation'
sftp_conn_id = 'sftp_pwc_absense_data_population'

log_filepath = '/PwCGlobal/ord/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_max_active_runs = 3
child_sync_create_max_active_runs = 1

schedule_timezone_Aukland = "Pacific/Auckland"
schedule_timezone_Paris = "Europe/Paris"
schedule_interval = "0 18 * * *"

error_template = '{{ get_error_message() }}'
