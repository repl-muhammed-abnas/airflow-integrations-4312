from horizonmedia.user_import_v2.mapper.holiday_calendar_mapper import horizonmedia_holiday_calendar_mapper

region = 'us-east-1'
environment = 'pre-production'

instance = "horizonmediatrial01"

company_key = 'horizonmediatrial01'
replicon_conn_id = 'horizonmediatrial01_replicon_admin'

can_run_batch_task_var_name = f'horizonmediatrial01_user_import_can_run_batch_task_{instance}'

user_list_report_name = "User list - For Integration"

# true only for qa testing . set this to false on prod - default false
can_use_conf_payload_var_name = f'horizonmedia_user_import_can_use_conf_payload_{instance}'
sftp_conn_id = f"sftp_horizonmedia_user_import_{instance}"
# sftp_ref_file_path = "/User Sync/Reference/horizonmedia_reference.csv"
sftp_ref_file_path = "/Test User Sync/Reference/horizonmedia_reference.csv"
sftp_archive_file_path = "/Test User Sync/Archive"
logpath = '/Test User Sync/Log Files/'
http_conn_id = f"http_horizonmedia_user_import_{instance}"

execution_timeout_days = 14
child_dag_max_active_runs = 20

# "timezone": "America/New_York",
# "cron_expression": "30 3 * * 1-5"
schedule_time_zone ='EST'
schedule_interval = '30 3 * * 1-5'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

horizonmedia_user_import_add_user_child = f'horizonmedia_user_import_add_user_child_v2_{instance}'
horizonmedia_user_import_disable_user_child = f'horizonmedia_user_import_disable_user_child_v2_{instance}'
horizonmedia_user_import_groups_check_child = f'horizonmedia_user_import_groups_check_child_v2_{instance}'
horizonmedia_user_import_master = f'horizonmedia_user_import_master_v2_{instance}'
horizonmedia_user_import_process_custom_fields_child = f'horizonmedia_user_import_process_custom_fields_child_v2_{instance}'
horizonmedia_user_import_supervisor_assignment_child = f'horizonmedia_user_import_supervisor_assignment_child_v2_{instance}'
horizonmedia_user_import_update_user_child = f'horizonmedia_user_import_update_user_child_v2_{instance}'

HORIZONMEDIA_HOLIDAY_CALENDAR_MAPPER = horizonmedia_holiday_calendar_mapper
