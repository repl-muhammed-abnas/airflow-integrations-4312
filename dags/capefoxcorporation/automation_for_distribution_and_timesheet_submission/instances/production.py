from capefoxcorporation.automation_for_distribution_and_timesheet_submission.config import *

instance = 'production'
environment = 'production'

company_key = 'capefoxcorporation'

replicon_conn_id = 'capefoxcorporation_replicon_admin'

tenant_email = 'csmith@infotekconsulting.net,culloa@capefoxss.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"capefoxcorporation_automation_for_distribution_and_timesheet_submission_master_{instance}"
child_process_timesheets_dag_id = f"capefoxcorporation_automation_for_distribution_and_timesheet_submission_process_timesheet_auto_populate_child_{instance}"
child_process_timesheet_submission_batch_dag_id = f"capefoxcorporation_automation_for_distribution_and_timesheet_submission_process_timesheet_submission_batch_child_{instance}"
