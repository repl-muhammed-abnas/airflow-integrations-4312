from ce_procore_integration.purchase_order_sync.utils.constants import InputSource


region = 'us-east-1'
environment = 'pre-production'

input_source = InputSource.SFTP

# SFTP configuration (used when input_source = 'sftp')
sftp_conn_id = 'dummy_sftp_conn_id'  # Override in instance file if using SFTP

# Email configuration (used when input_source = 'email')
imap_conn_id = 'computerease_procore_imap'  # Override in instance file
email_limit = 1
max_emails_to_check = 100
po_report_filename = 'Purchase Order Report'
email_subject_pattern = 'Purchase Order Report'

# DAG execution settings
execution_timeout_days = 7
main_dag_max_active_runs = 1
child_dag_max_active_runs = 5
sov_dag_max_active_runs = 5

# Purchase order sync specific settings
purchase_order_sync_interval_minutes = 10  # How often to run sync

# Procore WBS segment configuration
cost_code_segment_name = 'Cost Code'
cost_code_segment_type = 'cost_code'
cost_type_name = 'Cost Type'
cost_type_type = 'line_item_type'

# Common settings
time_format = '%Y-%m-%dT%H:%M:%SZ'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
