# pylint: disable=wildcard-import unused-wildcard-import
from michaelkorstna.timeoff_export_to_workday.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'Michaelkorstnaafmig'
replicon_conn_id = 'michaelkorstnaafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'
workday_isu_replicon_time_off_entries_http_conn_id = 'MichaelKorsTnA_workday_isu_replicon_time_off_entries_http_conn'
workday_isu_replicon_inbound_http_conn_id = 'MichaelKorsTnA_workday_isu_replicon_inbound_http_conn'

# Skip Workday API calls in trial - use mock data from Airflow variable instead
skip_workday_report_query = True
workday_report_mock_var_name = 'michaelkorstna_timeoff_export_workday_report_mock_data'

# Email configurations
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_support_email_cc = '{{ var.value.dagrun_internal_testing_email }}'

# File paths
timeoff_export_filepath = '/timeoffexport/'
archive_filepath = '/timeoffexport/Archive/'
log_filepath = '/timeoffexport/logs/'

export_file_prefix_new = 'MK_Timeoffexport_New'
export_file_prefix_delta = 'MK_Timeoffexport_Delta'

can_run_batch_task_var_name = f'michaelkorstna_timeoff_export_can_run_batch_task_{instance}'

# DAG IDs
master_dag_id = f'michaelkorstna_timeoff_export_to_workday_master_{instance}'
extract_new_bookings_child_dag_id = f'michaelkorstna_timeoff_export_to_workday_extract_new_child_{instance}'
extract_delta_bookings_child_dag_id = f'michaelkorstna_timeoff_export_to_workday_extract_delta_child_{instance}'
process_timeoff_records_to_workday_dag_id = f'michaelkorstna_timeoff_export_to_workday_process_records_child_{instance}'
send_logs_dag_id = f'michaelkorstna_timeoff_export_to_workday_send_logs_child_{instance}'
