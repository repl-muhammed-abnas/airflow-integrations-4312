region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_max_active_runs = 20
parallel_count = 10
TIME_ENTRY_BATCH_COUNT = 5

sumo_conn_id = 'sumologic-exportlogger'

# copy the below lines to the respective instance config if secondary sftp needs to be set up
secondary_sftp = False  # set this to True to enable secondary_sftp
if secondary_sftp:
    secondary_sftp_conn_id = 'sftp_useast2'
    secondary_log_filepath = '/PwCGlobal/Absense_Data_Population/logs'
