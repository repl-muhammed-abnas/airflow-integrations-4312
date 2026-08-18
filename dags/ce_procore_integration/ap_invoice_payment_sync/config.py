from ce_procore_integration.ap_invoice_payment_sync.utils.constants import InputSource

# Operation type constants
CREATE = 'create'
UPDATE = 'update'
DELETE = 'delete'

region = 'us-east-1'
environment = 'pre-production'

# Input source configuration (SFTP or EMAIL)
input_source = InputSource.SFTP

# Override in instance file
sftp_conn_id='dummy_id'
imap_conn_id = 'dummy_id'

file_path = '/dummy_path'
archive_file_path = '/dummy_archive_path'

email_limit = 1
max_emails_to_check = 100
email_subject_pattern = 'AP Invoice Payment Report'
ap_invoice_payment_report_filename = 'AP Invoice Payment Report'

# DAG execution settings
execution_timeout_days = 7
main_dag_max_active_runs = 1
child_dag_max_active_runs = 10
payment_dag_max_active_runs = 10
sftp_sensor_timeout_minutes = 10
schedule_interval_minutes = 60  # 1 hour for SFTP mode

# Common settings
ce_time_format = '%Y-%m-%dT%H:%M:%SZ'

# Retry configuration for failed payments
# Immediate (next run), 3h, 6h, 12h, 24h; failure beyond this count is terminal
retry_delays_hours = [0, 3, 6, 12, 24]
retry_buffer_minutes = 5  # Buffer when checking if retry time is reached
internal_email = ['procoreintegrationsupport@deltek.com']

s3_file_name = 'ap_invoice_payment_fingerprints.csv'
is_paused_upon_creation = True
