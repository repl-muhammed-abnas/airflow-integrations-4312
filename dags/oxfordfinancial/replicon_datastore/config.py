region = 'us-east-1'
environment = 'pre-production'

dag_max_active_tasks = 128
execution_timeout_days = 14

child_dag_process_append_time_entries = 10
subchild_dag_process_append_time_entries = 15

master_data_report_name = '***Replicon to Data Store'
client_data_report_name = '******TimeExtract Client'
service_data_report_name = '*****TimeExtract Service'
user_data_report_name = '******TimeExtract User'

extract_filepath = '/Production/Replicon_Time_Entries/Replicon_Time_Entries.csv'

sumo_conn_id = 'sumologic-dagrunlogger'

schedule_interval = '0 23 * * 3'
time_zone = 'EST'
