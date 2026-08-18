# Philippines-specific configuration file
# Contains hardcoded values not specific to individual instances

# defined in the main config file
region = "us-east-2"
environment = "pre-production"

execution_timeout_days = 14

# Valid company codes for Philippines
valid_company_codes = ('PHES', 'PHET')

# Philippines-specific UDFs (User Defined Fields)
PHILIPPINES_UDF = [
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

end_user_permission = "Employee"
supervisor_end_user_permission = "Manager"
supervisor_end_user_supervision_permission = "Approver"

DAG_BATCH_COUNT = 3
