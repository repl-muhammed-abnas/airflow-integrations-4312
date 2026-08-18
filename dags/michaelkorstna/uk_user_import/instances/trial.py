# pylint: disable=wildcard-import unused-wildcard-import
from michaelkorstna.uk_user_import.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'Michaelkorstnatrial01'
replicon_conn_id = 'michaelkorstnatrial01_replicon_radmin'
sftp_conn_id = "sftp_useast2"
workday_http_conn_id = 'michaelkorstna_user_import_workday_http_connection'
schedule_interval = '15 22 * * *'

max_active_runs_groups=1 #We cannot increase this
max_active_runs_child=5

time_zone = 'Etc/UTC'

tenant_email = 'Nishank.Jetley@michaelkors.com,Chetan.Chavre@michaelkors.com,Alex.Sage@michaelkors.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_support_email_cc = '{{ var.value.dagrun_internal_testing_email }}'

reference_filepath = '/michaelkorstna/uk/reference/'
archive_filepath = '/michaelkorstna/uk/archives/'


can_run_batch_task = f'michaelkorstna_uk_user_import_can_run_batch_task_{instance}'

disabled=True
