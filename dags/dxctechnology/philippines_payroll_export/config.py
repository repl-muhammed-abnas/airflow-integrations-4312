region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'

schedule_interval = '0 0 25,26 * *'

time_zone = "Asia/Kolkata"

max_dag_active_runs = 1
child_dag_max_active_runs = 1
execution_timeout_days = 14
write_csv_thread_pool_size = 2

division_names = ['PHES', 'PHET']

fileformat_name = 'Philippines Payroll Export'
report_name = 'Philippines_Payroll_Export_User_Data'
