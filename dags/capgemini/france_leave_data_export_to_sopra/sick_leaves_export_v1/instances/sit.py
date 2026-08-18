# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.france_leave_data_export_to_sopra.sick_leaves_export_v1.config import *
from capgemini.france_leave_data_export_to_sopra.sick_leaves_export_v1.mapper.timeoff_codes_sit import timeoff_codes

instance = 'sit'
location = 'France'

environment = 'pre-production'

company_key = 'capgeminisit'

schedule_interval = "0 1 * * *"

replicon_conn_id = 'capgeminisit_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiSIT'
pgp_conn_id = 'pgp_sopra_capgeminisit'

# pylint: disable=line-too-long
expected_approved_report_columns = "Employee ID;Booking Start Date;Booking End Date;Time Off Type;Booking Day;Time Off Hrs;Approval Status;Booking Uri;Initial Or Extension ?;Have you worked on the start date of the leave ?"
expected_deleted_report_columns = "Employee ID;Current Start Date;Current End Date;Current Time Off Type;Action;Booking Uri"

input_filepath = "/Outbound/FRA_Sopra_SickLeaves/Input"
s3_upload_filepath = "CapgeminiSIT/Outbound/FRA_Sopra_SickLeaves/Input"
filename_prefix = "Replicon_SIT_Sick_Leaves_FRA"

approved_leaves_report_name = "France 032B Sick Leaves - Approved"
deleted_leaves_report_name = "France 032B Sick Leaves - Deleted"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_france_leave_data_extract_sick_leaves_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_france_leave_data_extract_to_sopra_sick_leaves_master_{instance}_v1'
timeoff_paycodes = timeoff_codes
