from geografia.journal_sync_replicon_to_xero.config import *


instance = "prod"
environment = 'production'
company_key = 'geografia'
replicon_conn_id = 'standard_xero_Geografia_replicon'
sftp_conn_id = "sftp_geografia_prod"
xero_conn_id = "standard_xero_Geografia_xero"
execution_timeout_days = 14

tenant_email = "nicki@geografia.com.au"
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}" 

archive_filepath = "/geografia/journal_sync/prod/archive/"

master_dag_id = f'geografia_journal_sync_master_{instance}'
schedule_interval = "0 0 7 * *"
time_zone = 'America/Los_Angeles'
