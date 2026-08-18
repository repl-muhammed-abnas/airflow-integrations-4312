# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_row_v3_deleted_leaves.config import *
from capgemini.deleted_timeoff_booking_webhook_logging.instances.uat import tenant_wide_log_list as twl_list
instance = 'uat'
leave_status = 'deleted leaves'
location = 'ROW'

environment = 'pre-production'

company_key = 'capgeminiuat'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgeminiuat_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat'

# pylint: disable=line-too-long
expected_report_columns = "Leave Request ID;Employee ID;Local Employee Number;Current Time Off Type;Current Start Date;Current End Date;Modified On"

export_columns = ['Leave Request ID', 'Employee ID', 'Local Employee Number', 'Current Time Off Type', 'Current Start Date', 'Current End Date', 'Modified On']

input_filepath = "/Outbound/CancelledLeaveRequests/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/CancelledLeaveRequests/Input"
filename_prefix = "Uat_ROW_LeaveRequestsCancelled"

report_name = "GTM INT007 LeaveRequestsDeleted ROW"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

tenant_wide_log_list = twl_list
should_add_timeoff_balance = True

previous_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_for_previous_day_global_master_deleted_leaves_rest_of_world_{instance}_v3'
current_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_master_deleted_leaves_rest_of_world_{instance}_v3'

disabled=True
