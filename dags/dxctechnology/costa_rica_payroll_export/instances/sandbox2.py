# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.costa_rica_payroll_export.config import *

instance = 'DXCSandbox2'
company_key = 'DXCSandbox2'

sftp_conn_id = 'sftp-dxcsandbox2_auspayroll-628172'
pgp_conn_id = 'pgp_dxctechnology_costarica'
replicon_conn_id = 'dxcsandbox2_replicon_RepliconIntWDPayroll'

bucket_name = 'replicon.integrations_dxcsandbox_s3_bucket'
aws_conn_id = 'replicon.workato_S3_account'

bucket_folder = "replicon.costarica_bucket_folder"

upload_filepath = '/Test/Outbound/PayrollTime/Costa Rica/'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'dxctechnology_costa_rico_payroll_export_can_run_batch_task_{instance}'
