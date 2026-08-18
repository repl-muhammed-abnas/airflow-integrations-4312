#pylint: disable=wildcard-import unused-wildcard-import
from cbrefcg.user_billing_rate_update.config import *

instance = 'production'
region = 'us-east-2'

environment = 'production'

user_report_name = 'Active Users Billing Rate Data-For Integrations'
project_report_name = 'ProjectDetailsForUpdatingBillingRate'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

company_key = 'CBREFCGProduction'
replicon_conn_id = 'cbrefcg_replicon_apiuser'
sftp_conn_id = 'cbrfcg_sftp_uswest'

reference_file_path = 'CBREFCGProduction/Reference/reference.csv'
archive_reference_file_path = 'CBREFCGProduction/Reference/Archive/'

child_dag_id = f'cbrefcg_update_users_billing_rates_child_{instance}'
process_billing_rates_dag_id = f'cbrefcg_process_billing_rates_child_{instance}'
can_run_batch_task_var_name = f'cbrefcg_update_users_billing_rates_can_run_batch_task_{instance}'
