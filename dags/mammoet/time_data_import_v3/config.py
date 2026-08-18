region = 'eu-central-1'
environment = "pre-production"

user_base_report_name = "*** TimeEntry Sync User Base Report"
project_task_base_report_name = "*** TimeEntry Sync Project Base Report"

master_max_active_run = 1
execution_timeout_days= 14
execution_timeout_hours =14
child_max_active_run = 10
parallel_count=5

#pylint: disable=line-too-long
expected_user_report_columns = "Login Name,Employee ID,UserUri,User Start Date,User End Date,User Status,Location (Current) (Full Path)"
expected_project_report_columns = "ProjectUri,WBS/Order Name,WBS/Order Code,WBS/Order Status,CS Order Code,CS Order Name (Full Path),CS Order Status,TaskUri,Time & Expense Entry Type"
