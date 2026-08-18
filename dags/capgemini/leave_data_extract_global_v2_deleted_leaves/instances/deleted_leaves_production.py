# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_v2_deleted_leaves.config import *

instance = 'production'
leave_status = 'deleted leaves'
location = 'India'

environment = 'production'

company_key = 'capgemini'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgemini_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_capgemini'

# pylint: disable=line-too-long
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

previous_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_for_previous_day_global_master_deleted_leaves_india_{instance}_v2'
current_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_master_deleted_leaves_india_{instance}_v2'
