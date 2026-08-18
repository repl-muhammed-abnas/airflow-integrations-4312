region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'NPSGafmig'
schedule_interval = "0 0 * * 1"
report_name = 'NPSG workday Expense Report'
time_zone = "America/New_York"

# pylint: disable=line-too-long
expected_report_columns = 'Tracking Number,Expense Sheet Date,Incurred Date,Employee ID,Expense Code,Receipt,Expense Code Code,User Name,Expense Sheet Description,Reimbursement Amount - Currency,Reimbursement Amount - Amount'

execution_timeout_days = 14
max_active_runs_master = 1
