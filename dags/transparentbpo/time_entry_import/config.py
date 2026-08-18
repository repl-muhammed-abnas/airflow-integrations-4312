region = "us-east-1"
environment = "pre-production"

timezone = 'Etc/UTC'

process_parallel_count = 2
max_active_runs_master = 1
max_active_runs_child = 4
max_active_runs_log_gen_child = 1
execution_timeout_days = 14
file_sensor_timeout = 5 
PROCESS_USER_BATCH_COUNT = 5

column_mapping = {
    'Employee ID': 'employee_id', 
    'Work Date': 'work_date',
    'Start Time': 'start_time',
    'End Time': 'end_time',
    'Hours': 'hours',
    'Timesheet Category': 'timesheet_category',
    'Project': 'project',
    'Task': 'task',
    'Activity': 'activity',
}

entry_dateformat = '%m/%d/%Y'
expected_csv_columns = "Project Name,Task Name (Full Path),Employee ID,ProjectUri,TaskUri"
project_task_report_name = "Project And Task Time Import"