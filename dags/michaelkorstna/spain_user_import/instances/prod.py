# pylint: disable=wildcard-import unused-wildcard-import
from michaelkorstna.spain_user_import.config import *

instance = "prod"
environment = 'production'
company_key = 'MichaelKorsTnA'
replicon_conn_id = 'MichaelKorsTnA_replicon_radmin'
sftp_conn_id = "sftp_MichaelKorsTnA_648665"
workday_http_conn_id = 'MichaelKorsTnA_user_import_workday_http_connection'
schedule_interval = '0 22 * * *'

max_active_runs_groups=1 #We cannot increase this
max_active_runs_child=5

time_zone = 'Etc/UTC'

tenant_email = "wdintegrationsupport@michaelkors.com,Justine.Love@CapriHoldings.com,Christina.Rogado@CapriHoldings.com,Nishank.Jetley@michaelkors.com,Chetan.Chavre@michaelkors.com,Alex.Sage@michaelkors.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_support_email_cc = '{{ var.value.dagrun_internal_log_email }}'

reference_filepath = '/user_sync/spain/Reference/'
archive_filepath = '/user_sync/spain/Archive/'


can_run_batch_task = f'MichaelKorsTnA_spain_user_import_can_run_batch_task_{instance}'