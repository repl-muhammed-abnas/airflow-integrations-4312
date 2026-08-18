# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.france_place_of_work_export_to_sopra.config import *

instance = 'uat'
location = 'France'

environment = 'pre-production'

company_key = 'capgeminiuat'

schedule_interval = "0 1 10 * *"

replicon_conn_id = 'capgeminiuat_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_sopra_capgeminiuat'

# pylint: disable=line-too-long
expected_report_columns = "Employee ID;Entry Date;Place of Work (FRA)"

input_filepath = "/Outbound/FRA_Placeofwork_export/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/FRA_Placeofwork_export/Input"
filename_prefix = "Replicon_UAT_Work_FRA"

report_name = "France 032E Place of Work"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_france_place_of_work_extract_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_france_place_of_work_extract_to_sopra_master_{instance}'
export_child_dag_id = f'capgemini_france_place_of_work_extract_to_sopra_process_exports_child_{instance}'

disabled=True
