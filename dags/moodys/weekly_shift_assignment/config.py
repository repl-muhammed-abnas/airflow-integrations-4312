region = 'eu-central-1'
environment = 'pre-production'

company_key = 'MoodysEMEAafmig'
replicon_conn_id = 'replicon-moodysemeaafmig-admin'
sftp_conn_id = 'sftp_moodys_emea'

execution_timeout_days = 14
schedule_interval = '0 2 * * SUN'
time_zone = 'America/Denver'
max_active_runs = 1
child_max_active_runs = 5

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

shift_assignment_report = '**Shift Assignment Report**'
# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'
weekly_shift_log = 'moodys_weeklyshiftupdate_logs'

log_filepath = '/MoodysEMEA/weekly/shiftassignment/logs'
