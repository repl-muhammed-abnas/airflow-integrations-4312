# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.payroll_export_hgs.config import *

instance = 'dev'
environment = 'pre-production'

company_key = 'capgeminidev'

replicon_conn_id = 'capgeminidev_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiDev'
pgp_conn_id = 'pgp_capgeminidev'

input_filepath = "/Outbound/PayrollLeave_Request/Input"
s3_upload_filepath = "CapgeminiDev/Outbound/PayrollLeave_Request/Input"

tenant_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

required_timeoffs = f'capgemini_payroll_extract_required_timeoffs_{instance}'
