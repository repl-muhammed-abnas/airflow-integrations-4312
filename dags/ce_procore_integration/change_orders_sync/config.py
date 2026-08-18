region = 'us-east-1'
environment = 'pre-production'

input_source = 'sftp'  # Options: 'sftp', 'email'

execution_timeout_days = 7
main_dag_max_active_runs = 1
child_dag_max_active_runs = 5

interval_minutes = 10
procore_date_format = '%Y-%m-%d'
status_to_sync = 'Approved'
types_to_sync = 'Customer'

co_report_filename = 'QTool Change Order Report'
job_cost_detail_report_filename = 'QTool Job Cost Detail'

# SFTP configuration (used when input_source = 'sftp')
sftp_conn_id = 'dummy_sftp_conn_id'  # Override in instance file if using SFTP
file_path = '/ce_procore/change_orders'
archive_filepath = f'{file_path}/archive'

# Email configuration (used when input_source = 'email')
imap_conn_id = 'imap_co_sync'  # Override in instance file
email_subject_pattern = 'Change Order Report'
email_limit = 1
email_max_to_check = 100

change_reason = 'Allowance'

sftp_sensor_timeout_minutes = 10

should_allow_update = False # if True change order will be synced in draft state

revenue_cost_type = 'REVENUE'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
