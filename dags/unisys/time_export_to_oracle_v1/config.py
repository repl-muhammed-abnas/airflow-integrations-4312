"""
Configuration file for Unisys Time Export to Oracle Integration
Contains configurations for exporting time data from Replicon to Oracle

Based on the provided field mappings and transformation rules for Oracle export
"""

region = "us-east-1"
environment = "pre-production"

# DAG execution configuration
max_active_run_master = 1
execution_timeout_days = 14
write_csv_timeout_hours = 90
thread_pool_size_csv = 5

# Time zone configuration
timezone = 'Etc/UTC'

# Schedule configuration - Daily at 4:30 AM UTC, Monday to Friday only
schedule_interval = '30 04 * * 1-5'

time_export_file_format = 'Time Data Export - Oracle'

# Oracle export CSV header based on provided tabular data
oracle_export_header = [
    'Transaction Type',
    'Business Unit',
    'Third-Party Application Transaction Source',
    'Document',
    'Document Entry',
    'Expenditure Batch',
    'Batch Description',
    'Expenditure Item Date',
    'Person Name',
    'Person Number',
    'Human Resource Assignment',
    'Project Name',
    'Project Number',
    'Task Name',
    'Task Number',
    'Expenditure Type',
    'Expenditure Organization',
    'Quantity',
    'Unit of Measure',
    'Original Transaction Reference',
    'Context Category'
]