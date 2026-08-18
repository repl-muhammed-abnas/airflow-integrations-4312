# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.timesheet_auto_submission_v5.config import *

instance = 'PwCDev'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCDev'
replicon_conn_id = 'pwcdev-replicon-eu.automation'

location = 'europe'

timesheet_report_name = "**Timesheet_autosubmission_records_Europe**"
timesheet_with_timeoffhours_report_name = "Timesheet_autosubmission_withtimeoffhours_Europe"
timesheet_with_project_actuals_report_name = "**Timesheet_autosubmission_Project actuals_Europe"

version = 'v5'

tenant_email = 'PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

timeoff_hour_logic_child_dag_id = f'pwcglobal_timesheet_auto_submission_timeoff_hours_child_{instance}_{location}_{version}'
timeoff_hour_logic_master_dag_id = f'pwcglobal_timesheet_auto_submission_timeoff_hours_master_{instance}_{location}_{version}'
zero_hour_logic_child_dag_id = f'pwcglobal_timesheet_auto_submission_zero_hours_child_{instance}_{location}_{version}'
zero_hour_logic_master_dag_id = f'pwcglobal_timesheet_auto_submission_zero_hours_master_{instance}_{location}_{version}'
zero_hours_recalculate_tiemsheet_dag_id = f"pwcglobal_zero_hours_recalculate_timesheets_child_dag_{instance}_{location}_{version}"
