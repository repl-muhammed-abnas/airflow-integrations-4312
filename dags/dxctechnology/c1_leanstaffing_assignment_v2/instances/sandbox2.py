# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_leanstaffing_assignment_v2.config import *

instance = 'sandbox2'
version = "_v2"

environment = 'pre-production'

company_key = 'DXCSandbox2'

get_webhook_log_name = 'c1_leanstaffassignment_webhooks_sb2_v1'

replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntC1'
sftp_conn_id = 'dxcsandbox2-sftp-628172_C1'
secondary_sftp_conn_id = 'dxcsandbox2-sftp-628172_C1taskteam'
http_conn_id = 'dxcsandbox2_POQ_C1Leanstaffing'

output_filepath = '/Test/Outbound/C1LeanstaffingAssignment/Output/'
archive_filepath = '/backup'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

extract_report_name = 'Replicon to C1 Team Assignments extract'
report_filter_name = 'UDFFilter_Project6_Taskassignment_billingratechangedate'

can_send_result_var = f"dxctechnology_c1_leanstaffing_export_send_downstream_{instance}_v1"

team_rate_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook_{instance}_teamratemodified_secret'
team_dates_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook_{instance}_teamdatesmodified_secret'
team_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook_{instance}_teammodified_secret'

# Dag ID's

webhook_processor_dag_id = f'dxctechnology_c1_leanstaff_assignment_webhook_processor_{instance}{version}'
export_master_dag_id = f'dxctechnology_c1_leanstaff_assignment_export_master_{instance}{version}'
export_get_team_changes_child_dag_id = f'dxctechnology_c1_leanstaff_assignment_export_team_changes_{instance}{version}'
export_post_to_api_endpoint_child_dag_id = f'dxctechnology_c1_leanstaff_assignment_export_post_output_{instance}{version}'

can_run_batch_task_var_name = f'dxctechnology_c1_leanstaff_assignment_batch_task_{instance}'

max_webhook_processor_active_dag_runs = 10

# Enables the webhook-processor fast path + export-master bulk validation.
# Toggle the Airflow Variable below ('true'/'false'); rollback is instant.
# Other instances leave this unset (original per-event validation retained).
export_bulk_validation_var_name = f'dxctechnology_c1_leanstaff_assignment_bulk_validation_{instance}'
