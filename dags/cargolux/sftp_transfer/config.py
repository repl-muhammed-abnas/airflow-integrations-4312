from datetime import timedelta

region = 'eu-central-1'
environment = 'pre-production'
execution_timeout_days = 1
schedule_interval = '*/30 * * * *'  # Every 30 minutes
max_active_runs = 1  

# Child DAG settings
child_max_active_runs = 10  
batch_size = 1  

sensor_timeout_minutes = 2  
poke_interval_seconds = 30 

source_sftp_log_path = '/Logs/'  

dest_sftp_output_path = '/Input/'  

dag_version = 'v1'
dag_description = 'Transfers payroll files from Replicon internal SFTP to TMF SFTP'
