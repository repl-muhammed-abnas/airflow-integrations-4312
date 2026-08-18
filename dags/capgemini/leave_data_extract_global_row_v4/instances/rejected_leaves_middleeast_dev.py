# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_row_v4.config import *

instance = 'dev'
leave_status = 'rejected leaves'
location = 'Middle East'

environment = 'pre-production'

company_key = 'capgeminidev'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgeminidev_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiDev'
pgp_conn_id = 'pgp_capgeminidev'

# pylint: disable=line-too-long
expected_report_columns = "Leave Request ID;Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Booking Start Date;Booking End Date;Time Off Days;Time Off Hours;Units;Time Off Comments;Approver GGID;Approval Status;Submitted By;Submitted On;Modified By;Modified On;In Lieu Date;Reason for Special Leave;Reason (PL);01 - Booking Day (Start Day);02 - Booking Day (End Day)"

export_columns = ['Leave Request ID', 'Employee ID', 'Local Employee Number', 'Time Off Type',
                  'Time Off Type Description', 'Booking Start Date', 'Booking End Date', 'Time Off Days', 'Time Off Hours', 'Units',
                  'Time Off Comments', 'Approver GGID', 'Approval Status', 'Submitted By', 'Submitted On', 'Modified By',
                  'Modified On', 'In Lieu Date', 'Reason for Special Leave', 'Reason (PL)', '01 - Booking Day (Start Day)', '02 - Booking Day (End Day)']

input_filepath = "/Outbound/LeaveRequests/Input"
s3_upload_filepath = "CapgeminiDev/Outbound/LeaveRequests/Input"
filename_prefix = f"Dev_{location.replace(' ', '')}_LeaveRequestsRejected"

report_name = "GTM INT007 LeaveRequestsRejected ME"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

export_region = location.lower().replace(" ", "")

previous_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_for_previous_day_master_rejected_leaves_{export_region}_region_{instance}_v4'
current_day_leave_export_master_dag_id = f'capgemini_leave_data_extract_global_master_rejected_leaves_{export_region}_region_{instance}_v4'

disabled=True
