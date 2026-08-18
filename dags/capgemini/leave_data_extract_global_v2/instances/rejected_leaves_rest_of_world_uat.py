# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_v2.config import *

instance = 'uat'
leave_status = 'rejected leaves'
location = 'ROW'

environment = 'pre-production'

company_key = 'capgeminiuat'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgeminiuat_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat'

# pylint: disable=line-too-long
expected_report_columns = "Leave Request ID;Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Booking Start Date;Booking End Date;Time Off Days;Time Off Comments;Approver GGID;Approval Status;Submitted By;Submitted On;Modified By;Modified On;In Lieu Date;Reason for Special Leave;Reason (PL)"

export_columns = ['Leave Request ID', 'Employee ID', 'Local Employee Number', 'Time Off Type',
                  'Time Off Type Description', 'Booking Start Date', 'Booking End Date', 'Time Off Days',
                  'Time Off Comments', 'Approver GGID', 'Approval Status', 'Submitted By', 'Submitted On', 'Modified By',
                  'Modified On', 'In Lieu Date', 'Reason for Special Leave', 'Reason (PL)']

input_filepath = "/Outbound/LeaveRequests/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/LeaveRequests/Input"
filename_prefix = "Uat_ROW_LeaveRequestsRejected"

report_name = "GTM INT007 LeaveRequestsRejected ROW"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

previous_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_for_previous_day_global_master_rejected_leaves_rest_of_world_{instance}_v2'
current_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_master_rejected_leaves_rest_of_world_{instance}_v2'
