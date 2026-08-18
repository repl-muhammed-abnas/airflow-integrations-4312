# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.france_leave_data_export_to_sopra.other_leaves_export_v1.config import *
from capgemini.deleted_timeoff_booking_webhook_logging.instances.uat import tenant_wide_log_list as twl_list

instance = 'uat'
location = 'France'

environment = 'pre-production'

company_key = 'capgeminiuat'

schedule_interval = "0 1 * * *"

replicon_conn_id = 'capgeminiuat_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_sopra_capgeminiuat'

# pylint: disable=line-too-long
expected_approved_report_columns = "Employee ID;Booking Start Date;Booking End Date;Time Off Type;01 - Booking Day (Start Day);02 - Booking Day (End Day);Time Off Hrs;Approval Status;Booking Uri;Bookingdays"
expected_deleted_report_columns = "Employee ID;Current Start Date;Current End Date;Current Time Off Type;Action;Booking Uri"

input_filepath = "/Outbound/FRA_Sopra_OtherLeaves/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/FRA_Sopra_OtherLeaves/Input"
filename_prefix = "Replicon_UAT_Other_Leaves_FRA"

approved_leaves_report_name = "France 032A Other Leaves - Approved_V2"
deleted_leaves_report_name = "France 032A Other Leaves - Deleted"

tenant_wide_log_list = twl_list

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_france_leave_data_extract_other_leaves_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_france_leave_data_extract_to_sopra_other_leaves_master_{instance}_v1'
