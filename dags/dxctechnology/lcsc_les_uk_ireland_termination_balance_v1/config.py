region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

child_dag_max_active_runs = 4
child_dag_update_udf_max_active_runs = 10
execution_timeout_days = 14
write_csv_threadpool_size = 10

date_time_format = "%m/%d/%Y, %H:%M:%S"
time_00_30 = '00:30:00'
# pylint: disable=line-too-long
user_report_expected_report_columns = "User Name,Location (Current),UserUri,User End Date,TermExportedAUS"
termination_balance_expected_report_columns = "User Name,Time Off Type,Time Off Balance,TimeOffTypeUri,Employee ID,User End Date,Actual Employee ID,UserUri"

error_template = '{{ get_error_message() }}'
