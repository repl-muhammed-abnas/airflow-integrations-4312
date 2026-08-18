# pylint: disable=line-too-long
region = 'us-east-1'
environment = 'pre-production'

child_dag_max_active_runs = 16

max_active_runs = 1

max_active_dag_runs = 1

execution_timeout_days = 14

location = "USA"

time_zone = "America/New_York"

schedule_interval = "0 7 * * *"

schedule_interval_biweekly = "0 7 * * *"

date_time_format = "%m/%d/%Y, %H:%M:%S"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

report_column = "Employee ID,Time Off Type,Time Off Accrued,Time Off Taken,Time Off Balance,Employee Type (Current),Job Code,Location (Current),US Pay Group"

report_name = "Payroll_balance_US"

employee_types = ('Contingent Worker','Hourly_Temporary_Full-Time','Hourly_Temporary_Full-Time_Project','Hourly_Temporary_Part-Time','Hourly_Temporary_Part-Time_Project','Salaried OT Eligible_Temporary_Full-Time','Salaried OT Eligible_Temporary_Full-Time_Project','Salaried OT EligibleTemporary_Part-Time','Salaried OT EligibleTemporary_Part-Time_Project','Salaried_Temporary_Full-Time','Salaried_Temporary_Full-Time_Project')

job_code = ('R21VA','G04VB','B01VB','J01VA','J01VB','P09VB','J01SV','H03VB','F09VB','S02SV','Q02VB','P10EV','P10VB','P09SV','F09VA','P02VB','O03VB','Q30VA','Q23VB','M09VB','S01EV','N04SV','S01SV','M06VA','C02VA','E16VA','100CE','F09CF','J01CO','A01VB','E07VA','F02SV','F19VB','F18VB','C06VB','M06VB','G04EV','G14VB','G07VB','G10VA','H22SV','H02VB','H01VB','H09VA','I03SV','I03VA','L07SV','O07SV','R14VB','P09VA','J01EV','C07SVP','I03VB','F10VB')


sick_employee_types = ('Hourly_Regular_Full-Time','Hourly_Regular_Full-Time_Project','Hourly_Regular_Part-Time','Hourly_Regular_Part-Time_Project')

salaried_employee_types = ('Salaried OT Eligible_Regular_Full-Time','Salaried OT Eligible_Regular_Full-Time_Project','Salaried OT Eligible_Regular_Part-Time','Salaried OT Eligible_Regular_Part-Time_Project','Salaried OT Eligible_Temporary_Full-Time_Project','Salaried OT Eligible_Temporary_Part-Time','Salaried OT Eligible_Temporary_Part-Time_Project','Salaried_Regular_Full-Time','Salaried_Regular_Full-Time_Project','Salaried_Regular_Part-Time','Salaried_Regular_Part-Time_Project','Salaried_Temporary_Full-Time','Salaried_Temporary_Full-Time_Project','Salaried_Temporary_Part-Time','Salaried_Temporary_Part-Time_Project')

location_code = ('BETHSDA9','BETHSNIAID','BETHSNICHD','BETHNICHD4','BETHSNINDS','HMLTNNIAID','POOLESVLE3','POOLSVL08','POOLSNIAID','ROCKVNIAID')

thread_pool_size_write_csv = 50
