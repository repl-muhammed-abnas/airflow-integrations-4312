region = 'us-east-1'
environment = 'pre-production'

secondary_output_filepath = '/Test/Outbound/USA ADP Payroll'

export_location = "USA"

max_active_runs_batch_child = 1

payroll_export_file_format = 'US ADP Export'

export = "Yes"

max_active_runs = 1

execution_timeout_days = 14

time_zone = "America/New_York"

schedule_interval = "0 8,9,13 * * *"

gv_system_number = '1'

expire_time =7*24*60*60

employee_type = ('Hourly_Regular_Full-Time_Project','Hourly_Regular_Full-Time','Hourly_Regular_Part-Time',
                 'Hourly_Regular_Part-Time_Project','Hourly_Temporary_Full-Time','Hourly_Temporary_Full-Time_Project',
                 'Hourly_Temporary_Part-Time','Hourly_Temporary_Part-Time_Project')

# pylint: disable=line-too-long
error_template = '{{ result(get_failed_upstream_task_ids() | first_or_default, key="error") | attr_or_default(["response.body", "exc_message", ""], default="Unknown error occurred") }}'
thread_pool_size_write_csv = 50
