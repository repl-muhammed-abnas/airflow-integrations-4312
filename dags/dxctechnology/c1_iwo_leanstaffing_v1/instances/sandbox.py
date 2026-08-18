# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_iwo_leanstaffing_v1.config import *

instance = 'sandbox'
version = "_v1"

company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntC1'
sftp_conn_id = 'dxcsandbox-sftp-628172_C1'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/Test/Inbound/IWOLeanStaffing/Input'
archive_filepath = '/Test/Inbound/IWOLeanStaffing/Archive'
log_filepath = '/Test/Inbound/IWOLeanStaffing/Logs'

can_run_batch_task_var_name = f"dxc_c1_iwo_leanstaffing_run_batch_task_{instance}"

master_dag_id = f'dxctechnology_c1_iwo_leanstaffing_master_{instance}{version}'
process_records_dag_id = f'dxctechnology_c1_iwo_leanstaffing_process_each_record_{instance}{version}'
process_each_child = f'dxctechnology_c1_iwo_leanstaffing_automation_child_{instance}{version}'
process_create_billing_rate_dag_id = f'dxctechnology_c1_iwo_leanstaffing_child_create_billing_rate_{instance}{version}'
