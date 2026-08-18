# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_leanstaffing_assignment_v2.config import *

instance = 'trial'
version = "_v2"

environment = 'pre-production'

company_key = 'dxctrial01'

replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'sftp_useast2'
secondary_sftp_conn_id = 'sftp_useast2'
http_conn_id = 'dxctrial01-c1-leanstaffing-export-http'

output_filepath = '/Trial/Outbound/C1LeanstaffingAssignment/Output'
archive_filepath = '/Input'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_send_result_var = f"dxctechnology_c1_leanstaffing_export_send_downstream_{instance}{version}"

team_rate_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook_{instance}_teamratemodified_secret'
team_dates_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook_{instance}_teamdatesmodified_secret'
team_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook_{instance}_teammodified_secret'

# Dag ID's

webhook_processor_dag_id = f'dxctechnology_c1_leanstaff_assignment_webhook_processor_{instance}{version}'
export_master_dag_id = f'dxctechnology_c1_leanstaff_assignment_export_master_{instance}{version}'
export_get_team_changes_child_dag_id = f'dxctechnology_c1_leanstaff_assignment_export_team_changes_{instance}{version}'
export_post_to_api_endpoint_child_dag_id = f'dxctechnology_c1_leanstaff_assignment_export_post_output_{instance}{version}'

can_run_batch_task_var_name = f'dxctechnology_c1_leanstaff_assignment_batch_task_{instance}'

# Export-side throughput optimisation - TRIAL ONLY for now.
# Enables the webhook-processor fast path + export-master bulk validation.
# Toggle the Airflow Variable below ('true'/'false'); rollback is instant.
# Other instances leave this unset (original per-event validation retained).
export_bulk_validation_var_name = f'dxctechnology_c1_leanstaff_assignment_bulk_validation_{instance}'
