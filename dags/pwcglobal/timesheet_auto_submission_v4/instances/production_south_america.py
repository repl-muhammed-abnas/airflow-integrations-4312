# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.timesheet_auto_submission_v4.config import *
instance = 'PwC'
region = 'eu-central-1'
environment = 'production'

company_key = 'PwC'
replicon_conn_id = 'pwcglobal-replicon-eu.automation'

location = 'southamerica'

timesheet_report_name = "**Timesheet_autosubmission_records_South America**"
timesheet_with_timeoffhours_report_name = "Timesheet_autosubmission_withtimeoffhours_SAmerica"
timesheet_with_project_actuals_report_name = "**Timesheet_autosubmission_Project actualsSAmerica"

tenant_email = 'gbl_replicon_support_team@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

timeoff_hour_logic_child_dag_id = f'pwcglobal_timesheet_auto_submission_timeoff_hours_child_{instance}_{location}_v4'
timeoff_hour_logic_master_dag_id = f'pwcglobal_timesheet_auto_submission_timeoff_hours_master_{instance}_{location}_v4'
zero_hour_logic_child_dag_id = f'pwcglobal_timesheet_auto_submission_zero_hours_child_{instance}_{location}_v4'
zero_hour_logic_master_dag_id = f'pwcglobal_timesheet_auto_submission_zero_hours_master_{instance}_{location}_v4'
zero_hours_recalculate_tiemsheet_dag_id = f"pwcglobal_zero_hours_recalculate_timesheets_child_dag_{instance}_{location}_v4"
