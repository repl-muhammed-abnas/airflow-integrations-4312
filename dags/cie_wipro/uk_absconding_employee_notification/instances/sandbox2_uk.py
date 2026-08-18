# pylint: disable=wildcard-import unused-wildcard-import
from cie_wipro.uk_absconding_employee_notification.config import *

environment = "pre-production"
instance = "sandbox"
company_key = "wiprosandbox2"
replicon_conn_id = "wiprosandbox2"


report_filter_entrydate = "TimesheetPeriodFilter"
TO_DATE_RANGE_FILTER = "DateRangeFilter"
TO_TYPE_FILTER = "TimeOffTypeFilter"
annual_leave_filter_value = "3343"  # timeofftype Name: IE - Annual leave Assignee
# timeofftype Name: IE - Leave Without Pay (LWOP)
lwop_leave_filter_value = "3612"
time_off_comments_value = "Attendance/Effort Not Marked"
n_consecutive_days = 5
gpo_email_field_column_name = 'GPO ID'
hr_manager_email_field_column_name = 'HR Manager ID'
days = 30


email_distro_list = 'ashishtiwari@deltek.com,PradipKumar@deltek.com'
internal_logs_email = "ashishtiwari@deltek.com,PradipKumar@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


max_child_run = 3
execution_timeout_days = 7
chunk_size = 100
location = "UK"
can_run_batch_task_var_name = 'true'
firstReminder = "1"
secondReminder = "2"
thirdReminder = "3"
forthReminder = "4"
weekStartDay = "monday"
weekEndDay = "friday"


schedule_interval = "0 10 * * *"
time_zone = "Asia/Riyadh"
date_format = '%d.%m.%Y'
report_date_format = '%b %d, %Y'


to_report_name = "***BaseReport_EmployeeAbscond_UK"
