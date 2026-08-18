# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.costa_rica_payroll_export.config import *

instance = 'production'
environment = 'production'
company_key = 'DXCTechnology'

sftp_conn_id = 'sftp_dxctechnology_628172'
pgp_conn_id = 'pgp_dxctechnology_lcsc_payroll_export'
replicon_conn_id = 'dxctechnology_replicon_RepliconIntWDPayroll'

bucket_name = 'replicon.integrations_dxctechnology_s3_bucket'
aws_conn_id = 'replicon.workato_S3_account'

bucket_folder = "replicon.costarica_bucket_folder"

upload_filepath = '/Production/Outbound/Costa Rica/'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'dxctechnology_costa_rico_payroll_export_can_run_batch_task_{instance}'
