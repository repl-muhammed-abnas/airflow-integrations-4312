# This is Master Config DO not EDIT Anything here
# Update where you are importing this config file

region = "us-east-2"
environment = "pre-production"

execution_timeout_days = 14

schedule_interval = 30

process_log_generation_max_active_runs = 1
process_users_parallel_count = 20


can_use_batch_task_variable = "can_use_batch_task_variable"
process_disable_user_dag_count = 4
max_active_run_master = 1
parallel_dag_run_count = 4
process_users_max_active_runs = 5
process_time_off_accrual_mac_active_runs = 3

global_add_user_max_active_runs = 3
global_add_user_timeoff_assignment_max_active_runs = 3
global_update_user_max_active_runs = 2
global_update_user_timeoff_assignment_max_active_runs = 3

DXC_ERPS = ['global', 'gsap']

UDFs = ["Gender", "Continuous Service Date", "On Leave", "Personnel Area Description", "Personnel Area Code", "Job Activity Type", "FTE",
                    "FTE %", "International Assignee", "International assignee start date", "International assignee end date", "PERNER", "RUT", "Middle Name",
                    "Time Type", "Date of Birth", "Employee Group", "Work Shift", "Management Level", "IA PERNER ID", "Terms and Conditions",
                    "Termination Reason", "Termination Reason Code", "Employee Sub Group","assignment_type", "Annual Leave Anni. Date",
                    "LSL Anniversary Date",  "Personal Leave Anni. Date", "Weekly Scheduled Hours", "EE Group", "PSA User"]

personnel_sub_area_code_to_group = ["U04A", "U02A", "U05A", "U06A"]
state_to_group = ['California', 'Colorado', 'Nevada', 'Puerto Rico', 'Rhode Island']
batch_count = 2

COMPASS_ALL_ALLOWED_COUNTRIES = ('united states of america', 'india', 'costa rica', 'australia', 'ireland', 'portugal')
COMPASS_ALL_ALLOWED_COUNTRIES_EXCEPT_IRELAND = ('united states of america', 'india', 'costa rica', 'australia', 'portugal')
COMPASS_COUNTRIES = ('united states of america', 'india', 'costa rica', 'portugal')

COMPASS_ALL_ALLOWED_COUNTRIES_EXCEPT_INDIA = ('united states of america', 'costa rica', 'australia', 'ireland', 'portugal')
C1_COUNTRIES = ("united states of america", "canada")
gsap_company_codes_lcsc = ('3001', '3124', '1602' ,'3118')

# For some reason 
# aus_compass_company_codes = ('AUES') values is taken as AUES
# to avoid this adding ('AUES', 'AUES') rather than just ('AUES')
aus_compass_company_codes = ('AUES', 'AUES')

## costa_rica
max_active_run_process_each_users_costa_rica = 5
max_active_run_add_user_costa_rica = 5
max_active_run_add_user_timeoff_assignemnt_costa_rica = 5
max_active_run_update_user_costa_rica = 5
max_active_run_update_user_timeoff_assignment_costa_rica = 5
max_active_run_rehire_user_timeoff_assignement_costa_rica = 5

## india
max_active_run_process_each_users_india = 5
max_active_run_add_user_india = 5
max_active_run_add_user_timeoff_assignemnt_india = 5
max_active_run_update_user_ia_0_timeoff_assignment_india = 5
max_active_run_update_user_ia_1_timeoff_assignment_india = 5
max_active_run_ind_sick_casual_timeoff_assignment_india = 5
max_active_run_rehire_user_timeoff_assignement_costa_rica = 5
max_active_run_update_user_timeoff_assignment_india = 5
max_active_run_update_user_india = 5

## us_csc
max_run_add_user = 5
max_run_add_to_assignment = 5
max_run_update_user = 5
max_run_update_to_assignment = 5
max_run_rehire_to_assignment = 5
max_run_holiday_to_assignment = 5
max_run_sick_cal_to_assignment = 5
max_run_sick_non_cal_to_assignment = 5
max_run_puerto_rico_to_assignment = 5
max_run_process_each_users = 5
