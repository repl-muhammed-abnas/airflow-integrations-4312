region = 'eu-central-1'
environment = 'pre-production'

replicon_conn_id = 'pwcinternal-replicon-eu.automation'
sftp_conn_id = 'sftp_useast2'

log_filepath = '/PwCGlobal/Absense_Data_Population/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_max_active_runs = 20
parallel_count = 10
TIME_ENTRY_BATCH_COUNT= 10

bearer_token_var = 'pwc_webhook_absense_data_population_secret'

sumo_conn_id = 'sumologic-exportlogger'

# copy the below lines to the respective instance config if secondary sftp needs to be set up
secondary_sftp = False  # set this to True to enable secondary_sftp
if secondary_sftp:
    secondary_sftp_conn_id = 'sftp_useast2'
    secondary_log_filepath = '/PwCGlobal/Absense_Data_Population/logs'
