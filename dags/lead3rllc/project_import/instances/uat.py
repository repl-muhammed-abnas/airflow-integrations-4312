# pylint: disable=wildcard-import unused-wildcard-import
from lead3rllc.project_import.config import *

instance = 'uat'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'lead3rllctrial01'
replicon_conn_id = 'lead3rllctrial01_Integration.user'
sftp_conn_id = 'sftp_lead3rllc_696576'

sftp_input_filepath = '/UAT/Project Import/Input'
sftp_archive_filepath = '/UAT/Project Import/Archive'

tenant_email = "lydia.tuch@lead3r.com, Billing@lead3r.com,accounting@lead3r.com,ena.park@huddl3.group,lucas.reichart@huddl3.group"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f'lead3rllc_project_import_master_{instance}'

child_add_missing_values_in_replicon_dag_id = f'lead3rllc_project_import_add_missing_values_in_replicon_child_{instance}'
add_project_child_dag_id = f'lead3rllc_project_import_add_project_child_{instance}'
process_log_generation_dag_id = f'lead3rllc_project_import_process_log_generation_{instance}'

child_add_client_dag_id = f'lead3rllc_project_import_add_client_child_{instance}'
child_add_or_enable_dropdown_options_dag_id = f'lead3rllc_project_import_add_or_enable_dropdown_options_child_{instance}'
child_add_department_group_dag_id = f'lead3rllc_project_import_add_department_group_child_{instance}'

can_run_batch_task_var_name = f'lead3rllc_project_import_batch_task_var_{instance}'
