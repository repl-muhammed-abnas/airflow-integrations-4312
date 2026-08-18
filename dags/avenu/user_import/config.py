region = 'us-east-2'
environment = 'pre-production'

company_key = 'AvenuInsightsAnalyticstrial01'

master_dag_interval = 30
max_active_runs_process_each_records = 10
max_active_runs_process_new_user = 10
max_active_runs_process_time_off_assignment_new_user = 10
max_active_runs_process_supervisor_check = 10
max_active_runs_process_time_off_policy_new_user = 10
max_active_runs_process_time_off_assignment_update_user = 10
max_active_runs_process_time_off_policy_update_rehire_user = 10
max_active_runs_process_update_user = 10
max_active_runs_master = 1
execution_timeout_days = 14
execution_timeout_hours = 12

work_week_uri = "urn:replicon:day-of-week:monday"
employee_type_values = ['Exempt Full Time',
                        'Exempt Part Time', 'Intern Exempt', 'Temporary Exempt']

timeofftemplate = 'Time Off'
schedule_policy = 'Standard 8 hours M-F'

us_locations = ['Centreville, VA', 'Houston, TX', 'Birmingham, AL', 'Remote - AR', 'Remote - AL', 'Remote - LA', 'Monroe County, PA', 'Remote - TX',
                'Dallas, TX', 'Remote - NC', 'Salem, OR', 'Mandeville, LA', 'Remote - OH', 'Remote - MI', 'Remote - KY', 'Remote - NJ', 'Remote - GA',
                'Remote - MA', 'Remote - TN', 'Remote - OK', 'Remote - MN', 'Remote - VA',
                'Waite Park, MN', 'Germantown, TN', 'Quincy, MA', 'Remote - MO', 'Remote - WA', 'Remote - ME', 'Remote - CO', 'Remote - ID', 'Remote - FL',
                'New Orleans, LA', 'Remote - IL', 'Remote - UT', 'Remote - IA',
                'Remote - NH', 'Remote - PA', 'Remote - WV', 'Remote - ND', 'Remote - NV', 'Remote - OR', 'Remote - KS', 'Amherst, NH', 'Remote - SD',
                'Remote - VT', 'Pittsburgh, PA']

ca_locations = ['Fresno, CA', 'Irvine, CA', 'Orange County, CA',
                'Remote - CA', 'Siskiyou County, CA', 'Solano County, CA', 'Westlake, CA']

special_time_off_policy = ["[US] PTO 6.15", "[US] PTO 6.46", "[US] PTO 7.69", "[US] PTO 8",
                           "[CAN] Annual Leave 10", "[CAN] Annual Leave 1.67", "[CAN] Annual Leave 4.67",
                           "[CAN] Annual Leave 6.25", "[CAN] Annual Leave 6.67", "[CAN] Annual Leave 8.34"]

default_time_off_policy = ["[US] PTO CA Exempt", "[US] PTO CA Non-Exempt", "[US] PTO Exempt",
                           "[US] PTO Non-Exempt", "[CAN] Annual Leave", "[CAN] ON Annual Leave"]

accural_policy = ["[US] PTO CA Exempt", "[US] PTO CA Non-Exempt", "[US] PTO Exempt",
                  "[US] PTO Non-Exempt", "[CAN] Annual Leave", "[CAN] ON Annual Leave",
                  "[US] PTO 6.15", "[US] PTO 6.46", "[US] PTO 7.69", "[US] PTO 8",
                  "[CAN] Annual Leave 10", "[CAN] Annual Leave 1.67", "[CAN] Annual Leave 4.67",
                  "[CAN] Annual Leave 6.25", "[CAN] Annual Leave 6.67", "[CAN] Annual Leave 8.34",
                  "[CAN] Sick Leave 7.5", "[CAN] Sick Leave", "[US] NY AZ Sick Leave"]

timeoff_for_no_accrual = ['NY AZ', 'Annual', 'PTO']


# This Time-offs are manually added to the User profile
timeoff_to_ignore = ['Avenu Holiday']
