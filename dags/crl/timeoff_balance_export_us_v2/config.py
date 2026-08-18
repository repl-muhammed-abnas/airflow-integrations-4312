region = 'us-east-1'
environment = 'pre-production'

secondary_sftp_conn_id = 'sftp_useast2'
secondary_output_filepath = '/Test/Outbound/USA ADP Payroll'

export_location = "USA"

timeoff_report_name_weekly = "TimeOff Balance-Sick and Banked USA Weekly"
timeoff_report_name_biweekly = "TimeOff Balance-Sick and Banked USA Bi-weekly"
report_filter_name = "UDFFilter_User48_SickPayoutEligible"
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
