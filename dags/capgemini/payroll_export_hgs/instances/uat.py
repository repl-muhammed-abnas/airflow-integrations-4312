# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.payroll_export_hgs.config import *

instance = 'uat'
environment = 'pre-production'

company_key = 'capgeminiuat'

replicon_conn_id = 'capgeminiuat_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat'

input_filepath = "/Outbound/PayrollLeave_Request/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/PayrollLeave_Request/Input"

tenant_email = 'capgeminisupportreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

required_timeoffs = f'capgemini_payroll_extract_required_timeoffs_{instance}'
