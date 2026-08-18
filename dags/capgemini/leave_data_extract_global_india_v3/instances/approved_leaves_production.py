# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_india_v3.config import *

instance = 'production'
leave_status = 'approved leaves'
location = 'India'

environment = 'production'

company_key = 'capgemini'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgemini_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_capgemini'

# pylint: disable=line-too-long
expected_report_columns = "Leave Request ID;Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Booking Start Date;Booking End Date;Time Off Days;Time Off Hours;Units;Time Off Comments;Approver GGID;Approval Status;Submitted By;Submitted On;Modified By;Modified On;In Lieu Date;Reason for Special Leave;Reason (PL);01 - Booking Day (Start Day);02 - Booking Day (End Day)"

export_columns = ['Leave Request ID', 'Employee ID', 'Local Employee Number', 'Time Off Type',
                  'Time Off Type Description', 'Booking Start Date', 'Booking End Date', 'Time Off Days', 'Time Off Hours', 'Units',
                  'Time Off Comments', 'Approver GGID', 'Approval Status', 'Submitted By', 'Submitted On', 'Modified By',
                  'Modified On', 'In Lieu Date', 'Reason for Special Leave', 'Reason (PL)', '01 - Booking Day (Start Day)', '02 - Booking Day (End Day)']

input_filepath = "/Outbound/LeaveRequests/Input"
s3_upload_filepath = "Capgemini/Outbound/LeaveRequests/Input"
filename_prefix = "Prod_LeaveRequestsApproved"

report_name = "GTM INT007 LeaveRequestsApproved"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

previous_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_for_previous_day_global_master_approved_leaves_india_{instance}_v3'
current_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_master_approved_leaves_india_{instance}_v3'
