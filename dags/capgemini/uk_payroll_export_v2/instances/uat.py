# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.uk_payroll_export_v2.config import *
from capgemini.uk_payroll_export_v2.mappers.costcenters_uat import cost_centers
from capgemini.uk_payroll_export_v2.mappers.overtime_paycodes import overtime_paycodes
from capgemini.uk_payroll_export_v2.mappers.oncall_paycodes import oncall_paycodes

instance = 'uat'

environment = 'pre-production'

company_key = 'capgeminiuat'

replicon_conn_id = 'capgeminiuat_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_sopra_morocco_capgeminiuat'

oncall_input_filepath = "/Outbound/GBR_PayrollExport/Input/OnCall"
overtime_input_filepath = "/Outbound/GBR_PayrollExport/Input/Overtime"
s3_oncall_upload_filepath = "CapgeminiUAT/Outbound/GBR_PayrollExport/Input/OnCall"
s3_overtime_upload_filepath = "CapgeminiUAT/Outbound/GBR_PayrollExport/Input/Overtime"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

cost_centers_list = cost_centers
overtime_paycodes_list = overtime_paycodes
oncall_paycodes_list = oncall_paycodes

can_run_batch_task_var_name = f'capgemini_uk_payroll_export_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_uk_payroll_export_master_{instance}_v2'
create_export_child_dag_id = f'capgemini_uk_payroll_export_create_export_child_{instance}_v2'
overtime_export_child_dag_id = f'capgemini_uk_payroll_export_overtime_entries_export_child_{instance}_v2'
oncall_export_child_dag_id = f'capgemini_uk_payroll_export_oncall_entries_export_child_{instance}_v2'

disabled=True
