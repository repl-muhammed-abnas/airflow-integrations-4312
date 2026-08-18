# V1.4 July 28th 2025 Updated Document Sathishkumar M
# Change Request 28th July 2025: Scenario 1 - Transfer limit for assignees (5 days max)
#                                 Scenario 4 - Policy structure changes (yearly entitlement)
# V1.5 October 30th 2025 Updated Document
# Change Request 30th October 2025: Remove 5-day cap for Assignees - Transfer entire balance like Local Hire
# Previous CR 28th July 2025: Scenario 1 - Transfer limit for assignees (5 days max) - REMOVED
#                              Scenario 4 - Policy structure changes (yearly entitlement)
region = 'eu-central-1'
environment = "pre-production"
time_zone = "Etc/UTC"

DATE_DEFAULT_FORMAT = "%Y/%m/%d"

ANNUAL_LEAVE = "[CH] Annual Leave"
ANNUAL_LEAVE_PARTTIME = "[CH] Annual Leave Parttime"
ANNUAL_LEAVES_ASSIGNEES = "[CH] Annual leave (assignees)"
ANNUAL_LEAVE_ADDITIONAL = "[CH] Additional leave"


REQUIRED_TIMEOFF_TYPES = [ANNUAL_LEAVE, ANNUAL_LEAVE_PARTTIME, ANNUAL_LEAVES_ASSIGNEES, ANNUAL_LEAVE_ADDITIONAL]

country = "Switzerland"

expected_report_columns = "User Name,Time Off Type,Time Off Balance,User Start Date,Employee ID,Login Name,Country (Current),FTE,Onsite Direct Recruit"

schedule_interval_annual_leave = "0 1 1 1 *"

annual_leave_balance_report = "***Time Off Balance Report CH - Annual Leave***"

execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
process_users_for_timeoff_balance_transfer_parallel_dagruns_count = 10
