from capefoxcorporation.automation_for_distribution_and_timesheet_submission.config import *

instance = 'trial'

company_key = 'capefoxcorporationsb'

replicon_conn_id = 'capefoxcorporationsb_replicon_integration'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"capefoxcorporation_automation_for_distribution_and_timesheet_submission_{instance}"
child_process_timesheets_dag_id = f"capefoxcorporation_automation_for_distribution_and_timesheet_submission_process_timesheet_auto_populate_child_{instance}"
child_process_timesheet_submission_batch_dag_id = f"capefoxcorporation_automation_for_distribution_and_timesheet_submission_process_timesheet_submission_batch_child_{instance}"

disabled = True
