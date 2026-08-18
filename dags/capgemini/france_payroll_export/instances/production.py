# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.france_payroll_export.config import *
from capgemini.france_payroll_export.paycodes.sopra_paycodes import sopra_paycodes_list
from capgemini.france_payroll_export.paycodes.gfs_paycodes import gfs_paycodes_list
from capgemini.france_payroll_export.paycodes.gfs_desired_paycode_list import desired_paycode_mapper
instance = 'production'
location = 'France'

environment = 'production'

company_key = 'capgemini'

schedule_interval = "0 20 * * *"
replicon_conn_id = 'capgemini_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_sopra_capgemini'
gfs_pgp_conn_id = 'pgp_capgemini'

input_filepath_sopra = "/Outbound/France_Payroll_Export_SOPRA/Input"
s3_upload_filepath_sopra = "Capgemini/Outbound/France_Payroll_Export_SOPRA/Input"
input_filepath_gfs = "/Outbound/France_Payroll_Export_GFS/Input"
s3_upload_filepath_gfs = "Capgemini/Outbound/France_Payroll_Export_GFS/Input"
filename_prefix = "PROD"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_france_payroll_export_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_france_payroll_export_master_{instance}'
create_payroll_extract_child_dag_id = f'capgemini_france_payroll_export_create_export_child_{instance}'
sopra_export_child_dag_id = f'capgemini_france_payroll_export_to_sopra_child_{instance}'
gfs_export_child_dag_id = f'capgemini_france_payroll_export_to_gfs_child_{instance}'

sopra_paycodes = sopra_paycodes_list
gfs_paycodes = gfs_paycodes_list
desired_paycodes = desired_paycode_mapper
desired_paycodes_names = tuple(desired_paycodes.keys())