# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.uk_payroll_export_v1.config import *
from capgemini.uk_payroll_export_v1.mappers.costcenters import cost_centers
from capgemini.uk_payroll_export_v1.mappers.overtime_paycodes import overtime_paycodes
from capgemini.uk_payroll_export_v1.mappers.oncall_paycodes import oncall_paycodes

instance = 'dev'

environment = 'pre-production'

company_key = 'capgeminidev'

replicon_conn_id = 'capgeminidev_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiDev'
pgp_conn_id = 'pgp_sopra_morocco_capgeminidev'

oncall_input_filepath = "/Outbound/GBR_PayrollExport/Input/OnCall"
overtime_input_filepath = "/Outbound/GBR_PayrollExport/Input/Overtime"
s3_oncall_upload_filepath = "CapgeminiDev/Outbound/GBR_PayrollExport/Input/OnCall"
s3_overtime_upload_filepath = "CapgeminiDev/Outbound/GBR_PayrollExport/Input/Overtime"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

cost_centers_list = cost_centers
overtime_paycodes_list = overtime_paycodes
oncall_paycodes_list = oncall_paycodes

can_run_batch_task_var_name = f'capgemini_uk_payroll_export_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_uk_payroll_export_master_{instance}_v1'
create_export_child_dag_id = f'capgemini_uk_payroll_export_create_export_child_{instance}_v1'
overtime_export_child_dag_id = f'capgemini_uk_payroll_export_overtime_entries_export_child_{instance}_v1'
oncall_export_child_dag_id = f'capgemini_uk_payroll_export_oncall_entries_export_child_{instance}_v1'

disabled=True
