region = 'us-east-1'
environment = 'pre-production'

process_child_dag_max_active_runs = 100
parallel_trigger_dagrun_count = 50
secondary_sftp_conn_id = 'sftp_useast2'
secondary_output_filepath = '/Test/Outbound/USA ADP Payroll'

export_location = "USA"

timeoff_report_name = "TimeOff Balance-Sick and Banked USA"

# pylint: disable=line-too-long
error_template = '{{ result(get_failed_upstream_task_ids() | first_or_default, key="error") | attr_or_default(["response.body", "exc_message", ""], default="Unknown error occurred") }}'

encyrpt_file = False
max_active_runs = 1
execution_timeout_days = 14
child_dag_max_active_runs = 10
duration_days = 84

jan_1st_schedule_interval = "0 7 1 1 *"
dec_31st_schedule_interval = "0 7 31 12 *"
daily_schedule_interval = "0 7 * * *"
