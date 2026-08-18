# UK&I CSC-specific configuration file
# Contains hardcoded values not specific to individual instances

# defined in the main config file
region = "us-east-2"
environment = "pre-production"
execution_timeout_days = 14

# Valid company codes for UK&I CSC (GSAP-C1 & FTP)
valid_company_codes = (
    "IEEU",
    "IEES",
    "GBA5",
    "GBC5"
)

# UK&I CSC-specific UDFs (User Defined Fields)
uki_es_UDF = [
    "Gender", # gender
    "Continuous Service Date", # service_date
    "On Leave", # on_leave
    "Personnel Area Description", 
    "Personnel Area Code", 
    "Job Activity Type", # job_level (adding it for reference only)
    "FTE", # fte
    "FTE %", # fte_ptc
    "International Assignee", # is_ia
    "International assignee start date", # ia_start_date
    "International assignee end date",  # ia_end_date
    "PERNER",
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
    "PSA User",
    "Term Exported (AUS)",
    "IA PERNER ID"
]

UKI_OEFs = [
    "Additional Job Classifications",
    "Employee Representative Status",
    "Employee Representative Effective Date",
    "Default Weekly Hours"
]

# Permission settings
end_user_permission = "Employee"
supervisor_end_user_permission = "Manager"
supervisor_end_user_supervision_permission = "Approver"

# DAG configuration
DAG_BATCH_COUNT = 3
MAX_PARALLEL_TASKS = 5
max_active_run_update_user_timeoff = 5
max_active_run_update_user_uki_es = 5
max_active_run_process_each_users_uki_es = 5
max_active_run_add_user_uki_es = 5
max_active_run_add_user_timeoff_assignment_uki_es = 5
can_run_batch_task_var_name_uki_es = "can_run_batch_task_uki_es"

# File naming patterns
FULL_FILE_PATTERN = "WD_Replicon_Full_File_*.csv"
DELTA_FILE_PATTERN = "WD_Replicon_Delta_File_*.csv"

# Timesheet settings
PILOT_TIMESHEET_EFFECTIVE_DATE = "2026-04-01"
UAT_TIMESHEET_EFFECTIVE_DATE = "2025-04-01"

# Management levels that require special handling
MANAGEMENT_LEVELS_NO_TIMESHEET = ["L1", "L2"]

# Time off types for proration
UK_TIMEOFF_TYPES = {
    "annual_leave": "[UK] Annual Leave",
    "bought_leave": "[UK] Bought A/L",
    "sold_leave": "[UK] Sold A/L",
    "pt_annual_leave_hrs": "[UK] P/T Annual Leave Hrs",
    "pt_bought_leave_hrs": "[UK] P/T Bought A/L Hrs",
    "pt_public_holiday_hrs": "[UK] P/T Public Holiday Hrs",
    "pt_sold_leave_hrs": "[UK] P/T Sold A/L Hrs",
    "emp_representative": "[UK] Employee representative duties"
}

IRL_TIMEOFF_TYPES = {
    "annual_leave": "[IRL] Annual Leave",
    "bought_leave": "[IRL] Bought A/L",
    "sold_leave": "[IRL] Sold A/L",
    "pt_annual_leave_hrs": "[IRL] P/T Annual Leave Hrs",
    "pt_bought_leave_hrs": "[IRL] P/T Bought A/L Hrs",
    "pt_public_holiday_hrs": "[IRL] P/T Public Holiday Hrs",
    "pt_sold_leave_hrs": "[IRL] P/T Sold A/L Hrs",
    "emp_representative": "[IRL] Employee Representative Duties"
}

# Holiday calendars
HOLIDAY_CALENDARS = [
    "UK-England & Wales",
    "UK-FDS Holiday Calendar",
    "UK-Northern Ireland",
    "UK-Scotland",
    "Republic of Ireland"
]

# ERP identifiers
ERP_NAME = "GSAP"
ERP_SUFFIXES = {
    "GSAP": "GSAP",
    "COMPASS": "Compass",
    "C1": "C1",
    "FTP": "FTP"
}