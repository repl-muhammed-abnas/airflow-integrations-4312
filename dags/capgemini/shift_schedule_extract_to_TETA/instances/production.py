# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.shift_schedule_extract_to_TETA.config import *

instance = 'production'
environment = 'production'

company_key = 'capgemini'

replicon_conn_id = 'capgemini_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_shift_schedule_extract_teta_capgemini'

input_filepath = "/Outbound/TETA_Shift/Input"
s3_upload_filepath = "Capgemini/Outbound/TETA_Shift/Input"
current_month_filename_prefix = "PL_CURRENT_MONTH_SCHEDULES"
future_months_filename_prefix = "PL_FUTURE_MONTHS_SCHEDULES"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_shift_schedule_extract_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_shift_schedule_extract_to_teta_master_{instance}'
export_child_dag_id = f'capgemini_shift_schedule_extract_to_teta_process_export_child_{instance}'
