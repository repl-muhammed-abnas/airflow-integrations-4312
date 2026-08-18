# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_india_v4.config import *

instance = 'uat'
leave_status = 'leaves waiting for approval'
location = 'India'

environment = 'pre-production'

company_key = 'capgeminiuat'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgeminiuat_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat'

# pylint: disable=line-too-long
expected_report_columns = "Leave Request ID;Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Booking Start Date;Booking End Date;Time Off Days;Time Off Hours;Units;Time Off Comments;Approver GGID;Approval Status;Submitted By;Submitted On;Modified By;Modified On;In Lieu Date;Reason for Special Leave;Reason (PL);01 - Booking Day (Start Day);02 - Booking Day (End Day);Reason type"

export_columns = ['Leave Request ID', 'Employee ID', 'Local Employee Number', 'Time Off Type',
                  'Time Off Type Description', 'Booking Start Date', 'Booking End Date', 'Time Off Days', 'Time Off Hours', 'Units',
                  'Time Off Comments', 'Approver GGID', 'Approval Status', 'Submitted By', 'Submitted On', 'Modified By',
                  'Modified On', 'In Lieu Date', 'Reason for Special Leave', 'Reason (PL)', '01 - Booking Day (Start Day)', '02 - Booking Day (End Day)', 'Reason type']

input_filepath = "/Outbound/LeaveRequests/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/LeaveRequests/Input"
filename_prefix = "Uat_LeaveRequestsWaiting"

report_name = "GTM INT007 LeaveRequestsWaitingForApproval"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

version = "v4"

previous_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_for_previous_day_global_master_leaves_waiting_for_approval_leaves_india_{instance}_{version}'
current_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_master_leaves_waiting_for_approval_leaves_india_{instance}_{version}'

disabled=True
