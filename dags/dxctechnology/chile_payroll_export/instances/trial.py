# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.chile_payroll_export.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = "trial"
company_key = 'dxctrial01'

schedule_interval= '19 0 * * 1-5'

replicon_conn_id = "dxctrial01_replicon_RepliconIntC1"
pgp_conn_id = 'pgp_dxctechnology_chile_payroll_export'
sftp_conn_id = "sftp_useast2"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

output_filepath = "/DXC/chile_payroll_export/"
archive_filepath = "/DXC/chile_payroll_export/archive"

disable=True

disabled=True
