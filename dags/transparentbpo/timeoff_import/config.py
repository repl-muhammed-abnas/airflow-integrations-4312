environment = 'pre-production'
region = 'us-east-1'

schedule_interval = '0 */6 * * *'
time_zone = 'America/Denver'  # Mountain Time (US & Canada)

max_active_runs = 1
max_active_child_runs = 4
max_active_runs_final_logs = 1

execution_timeout_days = 14

final_log_generation_dag_schedule_interval = '0 1 * * *'  # 01:00 Mountain Time

daterange = 5  # Days before and after current date for BambooHR timeoff requests
timeoff_status = 'approved'  # BambooHR timeoff request status filter
allowed_timeoff_types = ['Vacation', 'Public & Bank Holidays']  # Timeoff types to process

DATE_DEFAULT_FORMAT = "%Y-%m-%d"

# Reference file settings for deduplication
dedup_retention_days = 30  # Days to retain records in reference file before cleanup
reference_filename = "timeoff_reference.csv"  # Fixed reference filename
reference_cleanup_schedule_interval = '0 2 1 * *'  # Run at 02:00 on 1st day of every month

# Reference file column mappings for CreateCollectionOperator
reference_file_columns = {
    'id': 'id',
    'dedup_key': 'dedup_key',
    'processed_date': 'processed_date'
}

# Reference file CSV header
reference_file_header = [
    'id',
    'dedup_key',
    'processed_date'
]
