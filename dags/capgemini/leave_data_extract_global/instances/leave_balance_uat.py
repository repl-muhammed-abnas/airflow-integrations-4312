# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global.config import *

instance = 'uat'
leave_status = 'leave balance'

environment = 'pre-production'

company_key = 'capgeminiuat'

schedule_interval = "0 0,4,8,12,16,20 * * *"

replicon_conn_id = 'capgeminiuat_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat'

# pylint: disable=line-too-long
expected_report_columns = "Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Leave Carry Forward;Leave Accrued;Leave Availed;Leave Reset;Leave Balance;Pushed On;User End Date"

export_columns = ['Employee ID', 'Local Employee Number', 'Time Off Type', 'Time Off Type Description',
                  'Leave Carry Forward', 'Leave Accrued', 'Leave Availed', 'Leave Reset', 'Leave Balance',
                  'Pushed On', 'User End Date']

input_filepath = "/Outbound/LeaveBalance/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/LeaveBalance/Input"
filename_prefix = "Uat_LeaveBalance"

report_name = "GTM INT007 FSD LeaveHeader (ISG DB)"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

disable=True

disabled=True
