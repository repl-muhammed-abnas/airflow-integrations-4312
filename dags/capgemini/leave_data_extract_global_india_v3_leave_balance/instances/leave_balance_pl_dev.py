# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_india_v3_leave_balance.config import *

instance = 'dev_PL'
leave_status = 'leave balance'
location = 'India'

environment = 'pre-production'

company_key = 'capgeminidev'

schedule_interval = "0 0,4,8,12,16,20 * * *"

replicon_conn_id = 'capgeminidev_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiDev'
pgp_conn_id = 'pgp_capgeminidev'

# pylint: disable=line-too-long
expected_report_columns = "Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Leave Carry Forward;Leave Accrued;Leave Availed;Leave Reset;Leave Balance;Units;Pushed On;User End Date"

export_columns = ['Employee ID', 'Local Employee Number', 'Time Off Type', 'Time Off Type Description',
                  'Leave Carry Forward', 'Leave Accrued', 'Leave Availed', 'Leave Reset', 'Leave Balance',
                  'Units', 'Pushed On', 'User End Date']

input_filepath = "/Outbound/LeaveBalance/Input"
s3_upload_filepath = "CapgeminiDev/Outbound/LeaveBalance/Input"
filename_prefix = "Dev_IND_PL_LeaveBalance"

report_name = "GTM INT007 FSD LeaveHeader(ISG DB) - India(PL)"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

leave_balance_export_master_dag_id = f'capgemini_leave_data_extract_global_master_leave_balance_india_privilege_leave_{instance}_v3'
