# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.payroll_leave_export_hgs_v2.config import *

instance = 'production'
environment = 'production'

company_key = 'capgemini'

replicon_conn_id = 'capgemini_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_payroll_leave_extract_capgemini'

input_filepath = "/Outbound/PayrollLeave_Request/Input"
s3_upload_filepath = "Capgemini/Outbound/PayrollLeave_Request/Input"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

required_timeoffs = f'capgemini_payroll_extract_required_timeoffs_{instance}'
can_run_batch_task_var_name = f'capgemini_payroll_extract_can_run_batch_task_{instance}'

export_file_prefix = "Prod"
