region = 'us-east-1'
environment = 'pre-production'
company_key = 'nttdatabctrial03'
schedule_interval = '0 23 * * *'
schedule_interval_master = '0 1 * * *'
timezone='America/Los_Angeles'
max_active_runs = 1
execution_timeout_days = 14
sumo_conn_id = 'sumologic-dagrunlogger'
employee_approved_timesheets="**Employee Approved Timesheets"
employee_pay_code_report="**Employee Pay Code Report**"

co_employee_approved_timesheets='Timesheet Period,User Name,Login Name,Activty Name,Hours Worked,Timeoff Type,\
Time Off Hrs,Total Hours (In Period),Approval Status,timesheeturi,useruri,Employee Type'
co_employee_pay_code='Timesheet Period,User Name,Login Name,Pay Code Name,Pay Code Code,Approval Status,Pay Code Hours,timesheet uri,useruri'
