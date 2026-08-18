# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.uk_payroll_export_v3.config import *
from capgemini.uk_payroll_export_v3.mappers.costcenters_production import cost_centers, cost_center_groups
from capgemini.uk_payroll_export_v3.mappers.overtime_paycodes import overtime_paycodes
from capgemini.uk_payroll_export_v3.mappers.oncall_paycodes import oncall_paycodes

instance = 'production'

environment = 'production'

company_key = 'capgemini'

replicon_conn_id = 'capgemini_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_sopra_uk_capgemini'

oncall_input_filepath = "/Outbound/GBR_PayrollExport/Input/OnCall"
overtime_input_filepath = "/Outbound/GBR_PayrollExport/Input/Overtime"
s3_oncall_upload_filepath = "Capgemini/Outbound/GBR_PayrollExport/Input/OnCall"
s3_overtime_upload_filepath = "Capgemini/Outbound/GBR_PayrollExport/Input/Overtime"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

# Keep original for any legacy references
cost_centers_list = cost_centers
# Use new grouped structure for the updated exports
cost_center_groups_list = cost_center_groups

overtime_paycodes_list = overtime_paycodes
oncall_paycodes_list = oncall_paycodes

can_run_batch_task_var_name = f'capgemini_uk_payroll_export_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_uk_payroll_export_master_{instance}_v3'
create_export_child_dag_id = f'capgemini_uk_payroll_export_create_export_child_{instance}_v3'
overtime_export_child_dag_id = f'capgemini_uk_payroll_export_overtime_entries_export_child_{instance}_v3'
oncall_export_child_dag_id = f'capgemini_uk_payroll_export_oncall_entries_export_child_{instance}_v3'
