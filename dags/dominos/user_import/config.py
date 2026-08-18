region = 'us-east-1'
environment = 'pre-production'

master_dag_interval = 30
execution_timeout_days = 14

master_dag_active_runs = 1
updateuser_child_dag_active_runs = 20

dag_max_active_tasks = 200

sumo_conn_id = 'sumologic-dagrunlogger'

pacific_timezone = 'America/Los_Angeles'

user_import_reference_report = 'User Import Reference'

input_filepath = '/Home/replicon/Pickup'
secondary_filepath = '/Daily Files'
