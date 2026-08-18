region = 'us-east-1'
environment = 'pre-production'

master_dag_interval = 30
execution_timeout_days = 14

master_dag_active_runs = 1
child_dag_active_runs = 3
child_dag_referencefile_active_runs = 5

dag_max_active_tasks = 200

input_filepath = '/OracleToReplicon'
archive_filepath = '/OracleToReplicon/Archive'
processing_filepath = '/OracleToReplicon/processing'
unprocessed_filepath = '/OracleToReplicon/unprocessed'
log_filepath = '/OracleToReplicon/Logs'

sumo_conn_id = 'sumologic-dagrunlogger'

bucket_name = 'replicon-integrations-uswest'
aws_conn_id = 'replicon.workato_S3_account'

master_user_reference_report = "***Master User Referece***"
user_import_reference_report = "***user import reference***"
