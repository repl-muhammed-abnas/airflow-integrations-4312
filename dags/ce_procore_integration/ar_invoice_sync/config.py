from ce_procore_integration.ar_invoice_sync.utils.constants import InputSource

region = 'us-east-1'
environment = 'pre-production'

input_source = InputSource.SFTP  

# DAG execution settings
execution_timeout_days = 7
main_dag_max_active_runs = 1
child_dag_max_active_runs = 1
prime_contract_sov_dag_max_active_runs = 10
invoice_dag_max_active_runs = 1

# AR invoice sync specific settings
ar_invoices_sync_interval_minutes = 30  # How often to run sync


# SFTP configuration (used when input_source = 'sftp')
sftp_conn_id = 'dummy_sftp_conn_id'  # Override in instance file if using SFTP
file_path = '/ce_procore/ar_invoices'
archive_file_path = f'{file_path}/archive'

# Email configuration (used when input_source = 'email')
imap_conn_id = 'computerease_procore_imap'  # Override in instance file
ar_invoice_report_filename = 'AR Invoice Report'
email_subject_pattern = 'AR Invoice Report'
email_limit = 1
email_max_to_check = 100

# Procore WBS segment configuration
cost_code_segment_name = 'Cost Code'
cost_code_segment_type = 'cost_code'
cost_type_name = 'Cost Type'
cost_type_type = 'line_item_type'

default_cost_type = 'M'

# Common settings
time_format = '%Y-%m-%dT%H:%M:%SZ'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
