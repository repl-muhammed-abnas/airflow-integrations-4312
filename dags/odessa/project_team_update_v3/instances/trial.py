from odessa.project_team_update_v3.config import *

instance = "trial"
environment = "pre-production"

company_key = "OdessaTrial01"

replicon_conn_id = "replicon_odessa_repliconint"
sftp_conn_id = "sftp_useast2"

master_dag_id = f"odessa_project_team_update_master_{instance}"
process_row_child_dag_id = f"odessa_project_team_update_process_row_child_{instance}"
assign_billing_rate_child_dag_id = f"odessa_project_team_update_assign_billing_rate_child_{instance}"

sftp_input_path = "/Odessa/odessateamupdate/Input"
sftp_archive_path = "/Odessa/odessateamupdate/Archive"
sftp_logs_path = "/Odessa/odessateamupdate/Logs"

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"