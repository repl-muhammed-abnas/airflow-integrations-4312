region = 'us-east-1'
environment = 'production'

instance = "daimlertrucks"

company_key = 'daimlertrucks'
replicon_conn_id = 'daimlertrucks_replicon_replicon'
sftp_conn_id = 'sftp_daimlertrucks_540697'
sftp_processedrecords_directory = "/Production/Datamart/ENG/Worker/ProcessedRecords"
sftp_archive_directory = "/Production/Datamart/ENG/Archive"
sftp_rejectedrecords_directory = "/Production/Datamart/ENG/Worker/RejectedRecords"

can_run_batch_task_var_name = f'{instance}_datamart_eng_export_can_run_batch_task'

empid_check_base_report_name = '***User-Employee ID Check***'
manager_eng_base_report_name = '***Manager ENG File***'

execution_timeout_days = 14
child_dag_max_active_runs = 20


schedule_time_zone = 'PST'
schedule_interval = '0 3 * * 1-5'

tenant_email = "Replicon-Support@daimlertruck.com,dtna-eng-timewiz@daimlertruck.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
