# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.payroll_leave_export_hgs_v3.config import *

instance = 'sit'
environment = 'pre-production'

company_key = 'capgeminisit'

replicon_conn_id = 'capgeminisit_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiSIT'
pgp_conn_id = 'pgp_payroll_leave_extract_capgeminisit'

input_filepath = "/Outbound/PayrollLeave_Request/Input"
s3_upload_filepath = "CapgeminiSIT/Outbound/PayrollLeave_Request/Input"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

required_timeoffs = f'capgemini_payroll_extract_required_timeoffs_{instance}'
can_run_batch_task_var_name = f'capgemini_payroll_extract_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_payroll_leave_export_hgs_generic_key_value_master_{instance}_v3'
process_approved_bookings_child_dag_id = f'capgemini_payroll_leave_export_hgs_generic_key_value_process_approved_bookings_child_{instance}_v3'
process_deleted_bookings_child_dag_id = f'capgemini_payroll_leave_export_hgs_generic_key_value_process_deleted_bookings_child_{instance}_v3'

export_file_prefix = "Sit"

disabled=True
