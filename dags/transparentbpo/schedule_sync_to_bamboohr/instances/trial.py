from transparentbpo.schedule_sync_to_bamboohr.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'transparentbpoafmig'
replicon_conn_id = 'transparentbpoafmig_replicon_replicon'
sftp_conn_id = 'sftp_useast2'

bamboohr_conn_id = f'transparent_bpo_schedule_sync_bamboohr_conn_id_{instance}'

master_dag_id = f'transparent_bpo_schedule_sync_master_{instance}'
process_scheduled_users_child_dag_id = f'transparentbpo_schedule_sync_process_scheduled_users_child_{instance}'
process_shift_users_child_dag_id = f'transparentbpo_schedule_sync_process_shift_users_child_{instance}'
post_to_bamboohr_dag_id = f'transparentbpo_schedule_sync_post_to_bamboohr_child_{instance}'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = "{{ var.value.dagrun_failure_alert_email }}"

log_filepath = "/transparentBPO/schedule_sync/Logs"
schedule_user_reference_filepath = "/transparentBPO/schedule_sync/Reference"
shift_user_reference_filepath = "/transparentBPO/schedule_sync/Reference"
schedule_user_reference_archive_filepath = "/transparentBPO/schedule_sync/Archive"
shift_user_reference_archive_filepath = "/transparentBPO/schedule_sync/Archive"


schedule_user_reference_filename_startswith = "scheduleusers"
shift_user_reference_filename_startswith = "shiftusers"
