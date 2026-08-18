from ce_procore_integration.util_dags.constants import InputSource

region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
main_dag_max_active_runs = 1
child_dag_max_active_runs = 5

# AP Invoice sync specific settings
invoice_sync_interval_minutes = 30  # How often to run sync

# Common settings
time_format = '%Y-%m-%dT%H:%M:%SZ'

skip_zero_amount_invoice = 'yes'
sftp_sensor_timeout_minutes = 10

input_source = InputSource.SFTP

# SFTP configuration (used when input_source = 'sftp')
sftp_conn_id = 'dummy_sftp_conn_id'  # Override in instance file if using SFTP
file_path = '/ce_procore/ap_invoices'
# Instance overrides of file_path must also override archive_filepath - this is not re-derived.
archive_filepath = f'{file_path}/archive'

# Email configuration (used when input_source = 'email')
imap_conn_id = 'computerease_procore_imap'  # Override in instance file
email_limit = 1
email_max_to_check = 100
email_subject_pattern = 'AP Invoice Detail Report'
ap_invoice_report_filename = 'QTool AP Invoice Detail'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
