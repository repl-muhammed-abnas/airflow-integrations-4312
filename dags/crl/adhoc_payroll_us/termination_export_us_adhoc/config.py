region = 'us-east-1'
environment = 'pre-production'

process_child_dag_max_active_runs = 100

secondary_output_filepath = '/Test/Outbound/USA ADP Payroll'

export_location = "USA"

max_active_runs_batch_child = 1

# pylint: disable=line-too-long
error_template = '{{ result(get_failed_upstream_task_ids() | first_or_default, key="error") | attr_or_default(["response.body", "exc_message", ""], default="Unknown error occurred") }}'
thread_pool_size_write_csv = 50
termination_balance_report_name = "User Details Reports USA"

payroll_export_file_format = 'US ADP Export'
export = "Yes"

max_active_runs = 1

execution_timeout_days = 14

time_zone = "America/New_York"

schedule_interval = "0 7 * * *"
