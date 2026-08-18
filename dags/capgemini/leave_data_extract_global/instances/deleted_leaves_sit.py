# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global.config import *

instance = 'sit'
leave_status = 'deleted leaves'

environment = 'pre-production'

company_key = 'capgeminisit'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgeminisit_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiSIT'
pgp_conn_id = 'pgp_capgeminisit'

expected_report_columns = "Leave Request ID;Employee ID;Local Employee Number;Current Time Off Type;Current Start Date;Current End Date;Modified On"

export_columns = ['Leave Request ID', 'Employee ID', 'Local Employee Number', 'Current Time Off Type', 'Current Start Date', 'Current End Date', 'Modified On']

input_filepath = "/Outbound/CancelledLeaveRequests/Input"
s3_upload_filepath = "CapgeminiSIT/Outbound/CancelledLeaveRequests/Input"
filename_prefix = "LeaveRequestsCancelled"

report_name = "GTM INT007 LeaveRequestsDeleted"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

disable=True

disabled=True
