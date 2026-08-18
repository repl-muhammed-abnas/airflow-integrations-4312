region = 'us-east-1'
environment = 'pre-production'

company_key = 'pike'

quick_base_report_name = 'CO - Quick Base Labor Entries'
head_count_report_name = 'Headcount Report - Telecom'
timeoff_report_name = 'Timeoff For Telecom'

quick_base_export_path = '/QuickBase Sync/Xcel/CO - Quick Base Labor Entries.csv'
head_count_export_path = '/Replicon Reports/Head Count Report/PROD/'
timeoff_export_path = '/Replicon Reports/Timeoff/PROD/'

max_active_runs = 1
mst_time_zone = 'MST7MDT'
est_time_zone = 'EST5EDT'
execution_timeout_days = 14
quick_base_export_schedule_interval = "0 2 * * *"
schedule_interval = "0 14 * * Tue"
