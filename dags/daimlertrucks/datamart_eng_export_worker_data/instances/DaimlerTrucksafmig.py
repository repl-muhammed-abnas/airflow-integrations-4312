region = 'us-east-1'
environment = 'pre-production'

instance = "DaimlerTrucksafmig"

company_key = 'DaimlerTrucksafmig'
replicon_conn_id = 'daimlertrucksafmig_replicon_admin'
# f"sftp_daimlertrucks_dta_eng_export_{instance}"
sftp_conn_id = 'sftp_klatrial_schedule_data_import'
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

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
