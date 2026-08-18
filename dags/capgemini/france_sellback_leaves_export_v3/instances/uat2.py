# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.france_sellback_leaves_export_v3.config import *
from capgemini.france_sellback_leaves_export_v3.mappers.codes_on_timeoffs import codes_to_export

instance = 'uat2'

environment = 'pre-production'

company_key = 'capgeminiuat2'

replicon_conn_id = 'capgeminiuat2_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_sopra_capgeminiuat2'

input_filepath = "/Outbound/France_RTT_CET_Sellback_Leaves_ExportUAT2/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/France_RTT_CET_Sellback_Leaves_ExportUAT2/Input"

expected_report_columns = "Employee ID,User Name,UserUri,Time Off Type,Units,Date,Event Type,Amount"
report_name = "France Sell Back Leaves Export V1"

max_active_runs = 1
execution_timeout_days = 14

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

filename_prefix = "Rep_CET"
codes_to_export_mapper = codes_to_export

can_run_batch_task_var_name = f'capgemini_france_sellback_leaves_export_can_run_batch_task_{instance}_v3'
master_dagid = f'capgemini_france_sellback_leaves_export_master_{instance}_v3'
export_child_dagid = f'capgemini_france_sellback_leaves_export_child_{instance}_v3'
