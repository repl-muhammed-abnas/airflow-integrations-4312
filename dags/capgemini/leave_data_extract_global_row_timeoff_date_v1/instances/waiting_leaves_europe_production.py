# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_row_timeoff_date_v1.config import *

instance = 'production'
leave_status = 'waiting for approval leaves'
location = 'Europe'

environment = 'production'

company_key = 'capgemini'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgemini_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_capgemini'

# pylint: disable=line-too-long
expected_report_columns = "Leave Request ID;Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Time Off Date;Booking Start Date;Booking End Date;Time Off Days;Time Off Hours;Units;Time Off Comments;Approver GGID;Approval Status;Submitted By;Submitted On;Modified By;Modified On;In Lieu Date;Reason for Special Leave;Reason (PL);01 - Booking Day (Start Day);02 - Booking Day (End Day);Reason type"

export_columns = ['Leave Request ID', 'Employee ID', 'Local Employee Number', 'Time Off Type',
                  'Time Off Type Description', 'Time Off Date', 'Booking Start Date', 'Booking End Date', 'Time Off Days', 'Time Off Hours', 'Units',
                  'Time Off Comments', 'Approver GGID', 'Approval Status', 'Submitted By', 'Submitted On', 'Modified By',
                  'Modified On', 'In Lieu Date', 'Reason for Special Leave', 'Reason (PL)', '01 - Booking Day (Start Day)', '02 - Booking Day (End Day)', 'Row Number', 'Reason type']

input_filepath = "/Outbound/LeaveRequests_TimeOffDate/Input"
s3_upload_filepath = "Capgemini/Outbound/LeaveRequests_TimeOffDate/Input"
filename_prefix = f"Prod_{location.replace(' ', '')}_LeaveRequestsWaiting_TODate"

report_name = "GTM INT007 LeaveRequestsWaiting EU TimeOffDate"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email}},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

export_region = location.lower().replace(" ", "")

version = "v1"

previous_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_for_previous_day_master_waiting_for_approval_leaves_timeoffdate_{export_region}_region_{instance}_{version}'
current_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_master_waiting_for_approval_leaves_timeoffdate_{export_region}_region_{instance}_{version}'
