from geografia.journal_sync_replicon_to_xero.config import *


instance = "trial"
environment = 'pre-production'
company_key = 'geografiaafmig'
replicon_conn_id = 'standard_xero_Geografiaafmig_replicon'
sftp_conn_id = "sftp_internal"
xero_conn_id = "standard_xero_Geografiaafmig_xero"
execution_timeout_days = 14


tenant_email = "{{ var.value.dagrun_internal_testing_email }}"  # change this when pushing the code to prod
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"  


archive_filepath = "/geografia/journal_sync/trial/archive"

master_dag_id = f'geografia_journal_sync_master_{instance}'
schedule_interval = "0 0 7 * *"
time_zone = 'America/Los_Angeles'
