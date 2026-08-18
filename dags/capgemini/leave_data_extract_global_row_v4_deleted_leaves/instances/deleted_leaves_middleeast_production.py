# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_row_v4_deleted_leaves.config import *
from capgemini.deleted_timeoff_booking_webhook_logging.instances.prod import tenant_wide_log_list as twl_list
instance = 'production'
leave_status = 'deleted leaves'
location = 'Middle East'

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
filename_prefix = f"Prod_{location.replace(' ', '')}_LeaveRequestsCancelled"

report_name = "GTM INT007 LeaveRequestsDeleted ME"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email}},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

tenant_wide_log_list = twl_list
should_add_timeoff_balance = True

export_region = location.lower().replace(" ", "")
previous_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_for_previous_day_master_deleted_leaves_{export_region}_region_{instance}_v4'
current_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_master_deleted_leaves_{export_region}_region_{instance}_v4'
