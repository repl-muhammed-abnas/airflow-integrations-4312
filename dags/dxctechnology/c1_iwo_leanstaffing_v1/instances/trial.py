# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_iwo_leanstaffing_v1.config import *

instance = 'trial'
version = "_v1"

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

input_filepath = '/Trial/Inbound/C1IWOLeanstaff/FilesToProcess'
archive_filepath = '/Trial/Inbound/C1IWOLeanstaff/Archive'
log_filepath = '/Trial/Inbound/C1IWOLeanstaff/Logs'

can_run_batch_task_var_name = f"dxc_c1_iwo_leanstaffing_run_batch_task_{instance}"

master_dag_id = f'dxctechnology_c1_iwo_leanstaffing_master_{instance}{version}'
process_records_dag_id = f'dxctechnology_c1_iwo_leanstaffing_process_each_record_{instance}{version}'
process_each_child = f'dxctechnology_c1_iwo_leanstaffing_automation_child_{instance}{version}'
process_create_billing_rate_dag_id = f'dxctechnology_c1_iwo_leanstaffing_child_create_billing_rate_{instance}{version}'
