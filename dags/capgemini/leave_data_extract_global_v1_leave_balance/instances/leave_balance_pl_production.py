# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_v1_leave_balance.config import *

instance = 'production_PL'
leave_status = 'leave balance'

environment = 'production'

company_key = 'capgemini'

schedule_interval = "0 0,4,8,12,16,20 * * *"

replicon_conn_id = 'capgemini_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_capgemini'

# pylint: disable=line-too-long
expected_report_columns = "Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Leave Carry Forward;Leave Accrued;Leave Availed;Leave Reset;Leave Balance;Pushed On;User End Date"

export_columns = ['Employee ID', 'Local Employee Number', 'Time Off Type', 'Time Off Type Description',
                  'Leave Carry Forward', 'Leave Accrued', 'Leave Availed', 'Leave Reset', 'Leave Balance',
                  'Pushed On', 'User End Date']

input_filepath = "/Outbound/LeaveBalance/Input"
s3_upload_filepath = "Capgemini/Outbound/LeaveBalance/Input"
filename_prefix = "Prod_IND_PL_LeaveBalance"

report_name = "GTM INT007 FSD LeaveHeader(ISG DB) - India(PL)"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'
