region = 'eu-central-1'
environment = "pre-production"
time_zone = "Etc/UTC"

DATE_DEFAULT_FORMAT = "%Y/%m/%d"

ANNUAL_LEAVE = "NL ODR-Annual leave"
ANNUAL_LEAVE_ADDITIONAL = "NL ODR-Additional Annual leave"
ANNUAL_LEAVE_CARRIED_OVER = "NL ODR-Annual leave Carried Over"

country = "Netherlands"

expected_report_columns = "User Name,Time Off Type,Time Off Balance,User Start Date,Employee ID,Login Name,Country (Current),Onsite Direct Recruit"

# Resource-group entitlements (V1.1). The yearly entitlement is split into a fixed
# legal portion (LEGAL_DAYS) and an extra-legal portion (entitlement - LEGAL_DAYS).
ODR_ENTITLEMENT = 25  # Onsite Direct Recruit (LOCAL_HIRE): 20 legal + 5 extra-legal
LTA_ENTITLEMENT = 22  # Assignee (ASSIGNEE): 20 legal + 2 extra-legal
LEGAL_DAYS = 20

# "Onsite Direct Recruit" report-field values mapped to resource groups.
ONSITE_DIRECT_RECRUIT_LOCAL_HIRE = "LOCAL_HIRE"
ONSITE_DIRECT_RECRUIT_ASSIGNEE = "ASSIGNEE"

schedule_interval_annual_leave = "0 1 1 1 *"

annual_leave_balance_report = "***Time Off Balance Report NL - Annual Leave***"

execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
process_users_for_timeoff_balance_transfer_parallel_dagruns_count = 20
time_off_accrual_script_name = 'Yearly/Monthly Accrual with Expiry & Rounding'
