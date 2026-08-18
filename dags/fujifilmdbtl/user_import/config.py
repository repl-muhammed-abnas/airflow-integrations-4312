region = 'us-east-1'
environment = 'pre-production'
time_zone = "Europe/Paris"

# master_dag_interval = 1 Uncomment this in prod and set accordingly
max_active_runs_master = 1
max_active_runs_child = 5

report_name = "Userlist for disabling User"
execution_timeout_days = 14

disable_threshold = 200
