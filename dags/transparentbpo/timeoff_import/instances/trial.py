from transparentbpo.timeoff_import.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'transparentbpoafmig'
replicon_conn_id = 'transparentbpoafmig_replicon_replicon'
sftp_conn_id = 'sftp_useast2'

bamboohr_conn_id = f'transparentbpo_timeoff_import_bamboohr_conn_id_{instance}'

master_dag_id = f'transparentbpo_timeoff_import_bamboohr_master_{instance}'
process_timeoff_bookings_child_dag_id = f'transparentbpo_timeoff_import_bamboohr_process_timeoff_bookings_child_{instance}'
process_each_timeoff_record_child_dag_id = f'transparentbpo_timeoff_import_bamboohr_process_each_timeoff_record_child_{instance}'
scheduled_log_generation_dag_id = f'transparentbpo_timeoff_import_bamboohr_scheduled_log_generation_master_{instance}'
reference_file_cleanup_dag_id = f'transparentbpo_timeoff_import_bamboohr_reference_file_cleanup_{instance}'

sftp_log_filepath = "/transparentBPO/timeoffimport"
# Reference file settings for deduplication
reference_filepath = "/transparentBPO/timeoffimport/reference/"
archive_filepath = "/transparentBPO/timeoffimport/reference/archive/"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"

can_run_batch_task = f'transparentbpo_timeoff_import_can_run_batch_task_{instance}'
can_run_cleanup_batch_task = f'transparentbpo_timeoff_import_can_run_cleanup_batch_task_{instance}'
