region = "us-east-1"

environment = "pre-production"

execution_timeout_days = 14
gather_user_logs_timeout_hours = 12

max_active_runs_process_user_import_payload = 1
max_active_runs_process_groups = 1
max_active_runs_process_buisness_unit = 1
max_active_runs_process_company_code = 1
max_active_runs_process_cost_center = 1
max_active_runs_process_location = 1
max_active_runs_process_new_departments = 1

max_active_runs_process_users = 20
max_active_runs_process_new_users = 10
max_active_runs_process_update_users = 10
max_active_runs_process_disable_users = 10
max_active_runs_process_supervisor = 20
max_active_runs_process_log_generation = 1

max_active_runs_process_timeoff_type_no_accrual = 20
max_active_runs_process_time_off_type_assignment_new_user = 10
max_active_runs_process_time_off_type_assignment_update_rehire_user = 10

disable_user_master_dag_active_runs = 1
disable_user_child_dag_active_runs = 2

trigger_parallel_dagrun_count_process_users = 15

ACTIVE_STATUS = ['Active','Paid Leave','Furlough','Dormant']
DISABLE_STATUS = ['Terminated','Unpaid Leave','Suspended','Retired','Discarted','Deceased']

BATCH_COUNT = 3

IGNORE_STATUS_ZERO_ACCRUAL = ['Unpaid Leave', 'Suspended']

DEFAULT_TIME_OFF_TYPE = "[UK] Annual leave"

APPLICABLE_TIME_OFF_TYPES = [
    "Holiday",
    "[UK] Neonatal leave",
    "[UK] Sick leave",
    "[UK] Maternity leave",
    "[UK] Adoption leave",
    "[UK] Paternity leave",
    "[UK] Carers leave",
    "[UK] Ordinary Parental leave",
    "[UK] Unpaid leave",
    "[UK] Jury Service",
    "[UK] Shared Parental Leave Unpaid",
    "[UK] Shared Parental Leave Paid",
    "[UK] Bought holiday",
    "[UK] KIT Day",
    "[UK] Annual leave",
    "[UK] Compassionate Leave",
    "[UK] Emergency time off for dependants",
    "[UK] Parental Bereavement leave",
    "[UK] Fostering leave",
    "[UK] Medical appointment",
    "[UK] Volunteering",
    "[UK] SPLIT Day",
    "[UK] Ante natal",
    "[UK] Special reserved forces",
    "[UK] Public duties",
    "[UK] Family emergency absence",
    "[UK] Garden leave",
    "[UK] Career break/sabbatical",
    "[UK] Other Absence",
    "[UK] Time Off in Lieu"
]

OT_ELIGIBLE_EMPLOYEE_TYPES = [
    "UK_Salaried OT Eligible_Full-Time_Project",
    "UK_Salaried OT Eligible_Full-Time",
    "UK_Salaried OT Eligible_Part-Time_Project",
    "UK_Salaried OT Eligible_Part-Time"
]

GLOBAL_TIME_OFF_TYPES = [
    "Holiday",
    "[UK] Neonatal leave",
    "[UK] Maternity leave",
    "[UK] Adoption leave",
    "[UK] Paternity leave",
    "[UK] Carers leave",
    "[UK] Ordinary Parental leave",
    "[UK] Unpaid leave",
    "[UK] Jury Service",
    "[UK] Shared Parental Leave Unpaid",
    "[UK] Shared Parental Leave Paid",
    "[UK] Bought holiday",
    "[UK] KIT Day",
    "[UK] Compassionate Leave",
    "[UK] Emergency time off for dependants",
    "[UK] Parental Bereavement leave",
    "[UK] Fostering leave",
    "[UK] Medical appointment",
    "[UK] Volunteering",
    "[UK] SPLIT Day",
    "[UK] Ante natal",
    "[UK] Special reserved forces",
    "[UK] Public duties",
    "[UK] Family emergency absence",
    "[UK] Garden leave",
    "[UK] Career break/sabbatical",
    "[UK] Other Absence"
]

PLACEHOLDER_BASED_TIMEOFF_TYPES = ["[UK] Annual leave", "[UK] Sick leave"]

TO_PLACEHOLDER_HIDDEN_OEF_NAMES = ["[UK] Annual leave - Placeholder Policy Name", "[UK] Sick leave - Placeholder Policy Name"]

#adding values to manual timeoff types you must make changes to global disable user manual timeoff type list
MANNUAL_TIMEOFF_TYPES = []

sumo_conn_id = 'sumologic-dagrunlogger'

END_DATE_STATUS = ['Terminated','Retired','Discarted','Deceased']

# For company 3300 employees already on a legacy "Pre 2010" pay rule, do not
# overwrite their pay rule during user updates (preserve historical assignment).
PROTECTED_3300_PAY_RULES = [
    '[UK] 3300 Pay Rule Pre 2010 FT OT',
    '[UK] 3300 Pay Rule Pre 2010 PT OT',
]

# Part-time time-off proration.
# Part-time employees receive Annual/Sick leave entitlement prorated by their
# working schedule instead of the full-time entitlement. Scope is limited to
# Annual leave and Sick leave for the companies below (3050 is NOT in scope).
# Formula: prorated = full_time_entitlement * (part_time_basis / full_time_basis)
# then rounded UP to the nearest 0.5 (e.g. 20.15 -> 20.5, 20.51 -> 21).
PRORATION_TIME_OFF_TYPES = ["[UK] Annual leave", "[UK] Sick leave"]

# Full-time WORKING DAYS per week (days-based proration). The part-time day count
# is derived from the employee's assigned time-off (office) schedule.
FT_WORKING_DAYS = {
    '3000': 5,
    '3080': 5,
    '3300': 5,
}

# Full-time standard WEEKLY HOURS (hours-based proration). The part-time hours
# come from the feed's Standard Hours (std_hrs) value.
FT_WEEKLY_HOURS = {
    '3040': 37.5,
}

# Schedule-based pay rule assignment (v.12): for these companies the pay rule name is
# suffixed with the employee's weekly hours (e.g. "[UK] 3000 Pay Rule 38hr") when such
# a rule is configured in Replicon; otherwise the mapper's base rule is used.
SCHEDULE_BASED_PAYRULE_COMPANIES = ['3000']

# Timesheet templates whose existing assignment must NOT be overwritten on update
# (legacy "Pre 2010" 3300 templates), analogous to PROTECTED_3300_PAY_RULES.
PRE2010_PROTECTED_TIMESHEET_TEMPLATES = [
    'UK_3300_Salaried_Project_Pre2010',
    'UK_3300_Salaried_NoProject_Pre2010',
]

# Base (non-OT) 3000 pay rule family: employees on this rule also receive
# "[UK] Time Off in Lieu" (in addition to OT_ELIGIBLE_EMPLOYEE_TYPES).
BASE_3000_PAY_RULE = '[UK] 3000 Pay Rule'
