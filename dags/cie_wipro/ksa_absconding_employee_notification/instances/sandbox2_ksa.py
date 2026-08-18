# pylint: disable=wildcard-import unused-wildcard-import
from cie_wipro.ksa_absconding_employee_notification.config import *

environment = "pre-production"
instance = "sandbox"
company_key = "wiprosandbox2"
replicon_conn_id = "wiprosandbox2"


report_filter_entrydate = "TimesheetPeriodFilter"
TO_DATE_RANGE_FILTER = "DateRangeFilter"
TO_TYPE_FILTER = "TimeOffTypeFilter"
annual_leave_filter_value = "934"
lwop_leave_filter_value = "1307"
time_off_comments_value = "Attendance/Effort Not Marked"
n_consecutive_days = 5
gpo_email_field_column_name = 'GPO ID'
days = 30
hr_manager_email_field_column_name = 'HR Manager ID'


HOLIDAY_DATE_RANGE_FILTER = "DateFilter"

email_distro_list = 'ashishtiwari@deltek.com'
internal_logs_email = "ashishtiwari@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


max_child_run = 3
execution_timeout_days = 7
chunk_size = 100
location = "KSA_Saudi"
can_run_batch_task_var_name = 'true'
firstReminder = "1"
secondReminder = "2"
thirdReminder = "3"
forthReminder = "4"
weekStartDay = "sunday"
weekEndDay = "thursday"


schedule_interval = "0 10 * * SUN"
time_zone = "Asia/Riyadh"
date_format = '%d.%m.%Y'
report_date_format = '%b %d, %Y'


to_report_name = "***BaseReport_KSAEmployeeAbscond"
hol_report_name = "***BaseReport_KSAEmployeeAbscond_Holiday"
