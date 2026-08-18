region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14
max_active_runs_master = 1
execution_timeout_mins_write_csv = 90
thread_pool_size = 2

schedule_interval = '30 12 1,2,8,9,10,L * *'

time_zone = "Etc/UTC"

report_name = 'UHR - India Shift Allowance'

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

expected_report_columns = "Employee Name,GGID,Emp Local ID,SBU Code,Employee Grade,PU,Project Code,Project Type,Request Start Date,Request End Date,Request Duration (Hrs),Project Manager,Submitted On,Modified On,Approver Name,Approver Employee Id,Approval Date,Location (Current)"
export_columns = ['Employee Name','GGID','Emp Local ID','SBU Code','Employee Grade','PU','Project Code','Project Type',\
        'Request Start_Date','Request End_Date','Request Duration','Project Manager','Submitted On','Modified On',\
        'Approver Name','Approver Employee_Id','Approval Date','Location Current']
