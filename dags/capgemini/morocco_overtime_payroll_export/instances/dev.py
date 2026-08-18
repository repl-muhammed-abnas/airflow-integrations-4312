# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.morocco_overtime_payroll_export.config import *

instance = 'dev'
location = 'Morocco'

environment = 'pre-production'

company_key = 'capgeminidev'

replicon_conn_id = 'capgeminidev_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiDev'
pgp_conn_id = 'pgp_sopra_morocco_capgeminidev'

input_filepath = "/Outbound/MAR_Overtime_ZY90/Input"
s3_upload_filepath = "CapgeminiDev/Outbound/MAR_Overtime_ZY90/Input"
ma01_filename_prefix = "ZY90_DEV_replicon_MA01"
ma02_ma03_filename_prefix = "ZY90_DEV_replicon_MA02_MA03"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_morocco_overtime_payroll_export_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_morocco_overtime_payroll_export_to_sopra_master_{instance}'
