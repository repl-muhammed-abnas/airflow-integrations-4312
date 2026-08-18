# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.france_payroll_export.config import *
from capgemini.france_payroll_export.paycodes.sopra_paycodes import sopra_paycodes_list
from capgemini.france_payroll_export.paycodes.gfs_paycodes import gfs_paycodes_list
from capgemini.france_payroll_export.paycodes.gfs_desired_paycode_list import desired_paycode_mapper
instance = 'sit'
location = 'France'

environment = 'pre-production'

company_key = 'capgeminisit'

schedule_interval = "0 20 * * *"
replicon_conn_id = 'capgeminisit_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiSIT'
pgp_conn_id = 'pgp_sopra_capgeminisit'
gfs_pgp_conn_id = 'pgp_capgeminisit'

input_filepath_sopra = "/Outbound/France_Payroll_Export_SOPRA/Input"
s3_upload_filepath_sopra = "CapgeminiSIT/Outbound/France_Payroll_Export_SOPRA/Input"
input_filepath_gfs = "/Outbound/France_Payroll_Export_GFS/Input"
s3_upload_filepath_gfs = "CapgeminiSIT/Outbound/France_Payroll_Export_GFS/Input"
filename_prefix = "SIT"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_france_payroll_export_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_france_payroll_export_master_{instance}'
create_payroll_extract_child_dag_id = f'capgemini_france_payroll_export_create_export_child_{instance}'
sopra_export_child_dag_id = f'capgemini_france_payroll_export_to_sopra_child_{instance}'
gfs_export_child_dag_id = f'capgemini_france_payroll_export_to_gfs_child_{instance}'

sopra_paycodes = sopra_paycodes_list
gfs_paycodes = gfs_paycodes_list
desired_paycodes = desired_paycode_mapper
desired_paycodes_names = tuple(desired_paycodes.keys())