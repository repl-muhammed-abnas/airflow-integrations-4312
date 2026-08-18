# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_v1_deleted_leaves.config import *

instance = 'prod'
leave_status = 'deleted leaves'

environment = 'production'

company_key = 'capgemini'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgemini_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_capgemini'

expected_report_columns = "Leave Request ID;Employee ID;Local Employee Number;Current Time Off Type;Current Start Date;Current End Date;Modified On"

export_columns = ['Leave Request ID', 'Employee ID', 'Local Employee Number', 'Current Time Off Type', 'Current Start Date', 'Current End Date', 'Modified On']

input_filepath = "/Outbound/CancelledLeaveRequests/Input"
s3_upload_filepath = "Capgemini/Outbound/CancelledLeaveRequests/Input"
filename_prefix = "Prod_LeaveRequestsCancelled"

report_name = "GTM INT007 LeaveRequestsDeleted"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'
tenant_wide_log = "capgemini_deleted_timeoffs_log"
should_add_timeoff_balance = True
