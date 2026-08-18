region = 'us-east-2'
environment = 'pre-production'

schedule_interval = "0 1 * * *"

master_dag_max_active_runs = 1

report_name = 'TimeEntrySubmission_For_All_Locations'

# To be updated to 30 while deploying to production
look_back_period_in_days = 30
