region = 'us-east-1'
environment = 'pre-production'
company_key = 'pimcoafmig'
replicon_conn_id = 'pimcoafmig_replicon_admin'

schedule_interval = '0 2 * * *'
schedule_interval_structure_update = '0 0 * * *'
max_active_runs_master=1
max_active_runs_child=1
max_active_runs_structure_update=1
pst_timezone = 'America/Los_Angeles'

execution_timeout_days=14
debug = False


project_name = 'Consultant Model Task'
all_project_task_report = "**Consultant Project Task Report"
