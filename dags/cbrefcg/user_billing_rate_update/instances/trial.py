#pylint: disable=wildcard-import unused-wildcard-import
from cbrefcg.user_billing_rate_update.config import *

instance = 'trial'
region = 'us-east-2'

environment = 'pre-production'

schedule_interval = '0 0 * * *'

user_report_name = 'Active Users Billing Rate Data-For Integrations'
project_report_name = 'ProjectDetailsForUpdatingBillingRate'

company_key = 'CBREFCGProductionafmig'
replicon_conn_id = 'cbrefcgafmig_replicon_apiuser'
sftp_conn_id = 'rsftp-useast_for_testing'

reference_file_path = '/cbrefcg/Reference/reference.csv'
archive_reference_file_path = '/cbrefcg/Reference/Archive/'

child_dag_id = f'cbrefcg_update_users_billing_rates_child_{instance}'
process_billing_rates_dag_id = f'cbrefcg_process_billing_rates_child_{instance}'
can_run_batch_task_var_name = f'cbrefcg_update_users_billing_rates_can_run_batch_task_{instance}'
disabled = True
