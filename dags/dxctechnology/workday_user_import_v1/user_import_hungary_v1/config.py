# defined in the main config file
region = "us-east-2"
environment = "pre-production"

execution_timeout_days = 14
file_sensor_timeout = 10

max_active_run_master = 1
max_active_run_process_each_users_hungary = 5
max_active_run_add_user_hungary = 5
max_active_run_add_user_timeoff_assignemnt_hungary = 5
max_active_run_update_user_hungary = 5
max_active_run_update_user_timeoff_assignment_hungary = 5
max_active_run_rehire_user_timeoff_assignement_hungary = 5
max_active_run_process_timeoff_no_accrual = 5
max_active_run_process_ia_1_timeoff_assignment = 5
max_active_run_process_ia_0_timeoff_assignment = 5
process_log_generation_max_active_runs = 1

process_users_parallel_count = 10

valid_company_codes = ('HU00')

end_user_permission = "Employee"
supervisor_end_user_permission = "Manager"
supervisor_end_user_supervision_permission = "Approver"

DAG_BATCH_COUNT = 3

# Hungary-specific UDFs (User Defined Fields)
HUNGARY_UDF = [
    "Gender", # gender
    "Continuous Service Date", # service_date
    "On Leave", # on_leave
    # "Personnel Area Description", 
    # "Personnel Area Code", 
    "Job Activity Type", # job_level (adding it for reference only)
    "FTE", # fte
    "FTE %", # fte_ptc
    "International Assignee", # is_ia
    "International assignee start date", # ia_start_date
    "International assignee end date",  # ia_end_date
    # "PERNER", 
    "Middle Name",
    "Time Type", # time_type 
    "Date of Birth", 
    # "Employee Group",
    "Work Shift",
    "Management Level",
    # "Terms and Conditions",
    # "Termination Reason",
    # "Weekly Scheduled Hours",
    # "EE Group",
    "assignment_type",
    "PSA User"
]