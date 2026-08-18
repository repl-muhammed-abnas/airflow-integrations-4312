# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.morocco_leave_data_export_to_sopra_v1.config import *
# take latest version of capgemini.deleted_timeoff_booking_webhook_logging
from capgemini.deleted_timeoff_booking_webhook_logging.instances.prod import tenant_wide_log_list as twl_list
from capgemini.morocco_leave_data_export_to_sopra_v1.mapper.timeoff_codes import timeoff_codes

instance = 'production'
location = 'Morocco'

environment = 'production'

company_key = 'capgemini'

replicon_conn_id = 'capgemini_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_sopra_morocco_capgemini'

input_filepath = "/Outbound/MAR_TOBookings_ZYAG/Input"
s3_upload_filepath = "Capgemini/Outbound/MAR_TOBookings_ZYAG/Input"
ma01_filename_prefix = "ZYAG_ABL_replicon_MA01"
ma02_ma03_filename_prefix = "ZYAG_ERD_replicon_MA02_MA03"

approved_leaves_report_name = "Morocco ZYAG Leaves - Approved"
deleted_leaves_report_name = "Morocco ZYAG Leaves - Deleted"

timeoff_paycodes = timeoff_codes

tenant_wide_log_list = twl_list

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_morocco_leave_data_extract_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_morocco_leave_data_extract_to_sopra_master_{instance}_v1'
