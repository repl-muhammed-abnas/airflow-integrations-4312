# pylint: disable=wildcard-import unused-wildcard-import
from grouppmx.salesforce_time_transfer.config import *

instance = "uat"

region = 'us-east-1'
environment = 'pre-production'
company_key = 'grouppmxtrial01'

replicon_conn_id = "grouppmxtrial01-replicon-admin"
salesforce_conn_id = 'grouppmxtrial01_salesforce_sandbox'

tenant_email = 'hcardozo@grouppmx.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'cshare_grouppmx_qubDDAr9Xe4F'

log_filepath ='/timetransfer_logs/'

can_run_batch_task_var_name = f'grouppmxtrial01_time_transfer_{instance}_can_run_batch_task'

master_dag_id = f'grouppmx_time_transfer_to_salesforce_master_{instance}'
client_dag_id = f'grouppmx_time_transfer_to_salesforce_process_client_child_{instance}'
project_dag_id = f'grouppmx_time_transfer_to_salesforce_process_project_child_{instance}'
timesheet_dag_id = f'grouppmx_time_transfer_to_salesforce_process_timesheet_child_{instance}'
delete_time_entry_dag_id = f'grouppmx_time_transfer_to_salesforce_process_delete_time_entry_child_{instance}'
timeentry_dag_id = f'grouppmx_time_transfer_to_salesforce_process_each_time_entry_child_{instance}'

disabled=True
