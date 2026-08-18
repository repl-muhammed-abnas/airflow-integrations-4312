# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.france_payroll_export.config import *
from capgemini.france_payroll_export.paycodes.sopra_paycodes import sopra_paycodes_list
from capgemini.france_payroll_export.paycodes.gfs_paycodes import gfs_paycodes_list
from capgemini.france_payroll_export.paycodes.gfs_desired_paycode_list import desired_paycode_mapper
instance = 'uat2'
location = 'France'

environment = 'pre-production'

company_key = 'capgeminiuat2'

schedule_interval = "0 20 * * *"
replicon_conn_id = 'capgeminiuat2_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_sopra_capgeminiuat2'
gfs_pgp_conn_id = 'pgp_capgeminiuat2'

input_filepath_sopra = "/Outbound/France_Payroll_Export_SOPRAUAT2/Input"
s3_upload_filepath_sopra = "CapgeminiUAT/Outbound/France_Payroll_Export_SOPRAUAT2/Input"
input_filepath_gfs = "/Outbound/France_Payroll_Export_GFSUAT2/Input"
s3_upload_filepath_gfs = "CapgeminiUAT/Outbound/France_Payroll_Export_GFSUAT2/Input"
filename_prefix = "UAT2"

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
