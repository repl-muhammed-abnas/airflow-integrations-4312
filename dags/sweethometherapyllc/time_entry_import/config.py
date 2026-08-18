region = "us-east-1"
environment = "pre-production"

timezone = 'Etc/UTC'

process_parallel_count = 5
max_active_runs_master = 1
max_active_runs_child = 5
max_active_runs_log_gen_child = 1
execution_timeout_days = 14
file_sensor_timeout = 5 

column_mapping = {
    'Entry_KEYID': 'entry_keyid',
    'School': 'school',
    'Identity ID': 'identity_id',
    'Last Name': 'last_name',
    'Therapist': 'therapist',
    'Service_KEYID': 'service_keyid',
    'Date of Service': 'date_of_service',
    'Service Name': 'service_name',
    'Hours': 'hours',
    'Type1': 'type1',
    'Type2': 'type2',
    'Therapy Start': 'therapy_start',
    'Therapy End': 'therapy_end',
    'Cancellation Notification Time': 'cancellation_notification_time',
    'Session Narrative': 'session_narrative',
    'Goals Progress': 'goals_progress',
    'Inserted On': 'inserted_on',
    'Error': 'error',
    'Error Details': 'error_details',
    'Queued for an Invoice?': 'queued_for_invoice',
    'Already on Invoice': 'already_on_invoice',
    'Invoice Status': 'invoice_status',
    'Alt 1 Service Name': 'alt1_service_name',
    'Alt 1 Hours': 'alt1_hours',
    'Alt 2 Service Name': 'alt2_service_name',
    'Alt 2 Hours': 'alt2_hours',
    'Num Students': 'num_students'
}

entry_dateformat = '%m/%d/%Y'
time_format = '%H:%M:%S'
scheduler_interval = "0 0 2,16 * *"
timesheet_dist = 'Time Punches with Distribution'
timesheet_approve_remarks = "Timesheet is force approved by Integration (Time Data Import)"