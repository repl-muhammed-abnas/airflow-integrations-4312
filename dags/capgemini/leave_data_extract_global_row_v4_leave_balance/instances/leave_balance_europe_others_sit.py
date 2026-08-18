# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_row_v4_leave_balance.config import *

instance = 'sit'
leave_status = 'leave balance'
location = 'Europe'
region_identifier = 'others'  # Set 5: SWEDEN, PORTUGAL, BELGIUM, NORWAY, UKRAINE, ROMANIA, FINLAND, SWITZERLAND, LUXEMBOURG, DENMARK, IRELAND, AUSTRIA (14,676 users)
countries = ['SWEDEN', 'PORTUGAL', 'BELGIUM', 'NORWAY', 'UKRAINE', 'ROMANIA', 'FINLAND', 'SWITZERLAND', 'LUXEMBOURG', 'DENMARK', 'IRELAND', 'AUSTRIA']

environment = 'pre-production'

company_key = 'capgeminisit'

schedule_interval = "0 1 */1 * *"

replicon_conn_id = 'capgeminisit_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiSIT'
pgp_conn_id = 'pgp_capgeminisit'

# pylint: disable=line-too-long
expected_report_columns = "Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Leave Carry Forward;Leave Accrued;Leave Availed;Leave Reset;Leave Balance;Units;Pushed On;User End Date"

export_columns = ['Employee ID', 'Local Employee Number', 'Time Off Type', 'Time Off Type Description',
                  'Leave Carry Forward', 'Leave Accrued', 'Leave Availed', 'Leave Reset', 'Leave Balance',
                  'Units', 'Pushed On', 'User End Date']

input_filepath = "/Outbound/LeaveBalance/Input"
s3_upload_filepath = "CapgeminiSIT/Outbound/LeaveBalance/Input"
filename_prefix = f"Sit_{location.replace(' ', '')}_LeaveBalance"
filename_seconds_suffix = "25"

report_name = "GTM INT007 FSD LeaveHeader(ISG DB) - EU - OTHERS"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

export_region = f"europe_{region_identifier}"
leave_balance_export_master_dag_id = f'capgemini_leave_data_extract_global_master_leave_balance_{export_region}_region_{instance}_v4'

disabled=True
