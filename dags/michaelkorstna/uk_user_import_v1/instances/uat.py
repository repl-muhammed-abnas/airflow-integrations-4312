# pylint: disable=wildcard-import unused-wildcard-import
from michaelkorstna.uk_user_import_v1.config import *

instance = "uat"
environment = 'pre-production'
company_key = 'MichaelKorsTnAsandbox'
replicon_conn_id = 'MichaelKorsTnAsandbox_replicon_radmin'
sftp_conn_id = "sftp_useast2"
workday_http_conn_id = 'michaelkorstna_user_import_workday_http_connection'
schedule_interval = '15 22 * * *'

max_active_runs_groups=1 #We cannot increase this
max_active_runs_child=5

time_zone = 'Etc/UTC'

tenant_email = 'Nishank.Jetley@michaelkors.com,Chetan.Chavre@michaelkors.com,Alex.Sage@michaelkors.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_support_email_cc = '{{ var.value.dagrun_internal_testing_email }}'

reference_filepath = '/michaelkorstna/uk/reference/'
archive_filepath = '/michaelkorstna/uk/archives/'


can_run_batch_task = f'michaelkorstna_uk_user_import_can_run_batch_task_{instance}'

version = '_v1'
master_dag_id = f'michaelkorstna_uk_user_import_master_{instance}{version}'
add_user_child_dag_id = f'michaelkorstna_uk_user_import_add_user_child_{instance}{version}'
user_update_child_dag_id = f'michaelkorstna_uk_user_update_child_{instance}{version}'
workflow_add_timeoff_child_dag_id = f'michaelkorstna_uk_child_workflow_to_add_timeoff_type_for_new_user_child_{instance}{version}'
holiday_proration_child_dag_id = f'michaelkorstna_uk_user_import_timeoff_type_uk_holiday_proration_assignment_child_{instance}{version}'
disable_users_child_dag_id = f'michaelkorstna_uk_user_import_disable_users_child_{instance}{version}'
groups_update_child_dag_id = f'michaelkorstna_uk_groups_update_child_{instance}{version}'
cost_center_add_child_dag_id = f'michaelkorstna_uk_user_import_cost_center_add_child_{instance}{version}'
department_add_child_dag_id = f'michaelkorstna_uk_user_import_department_add_child_{instance}{version}'
location_add_child_dag_id = f'michaelkorstna_uk_location_add_child_{instance}{version}'
employee_type_add_child_dag_id = f'michaelkorstna_uk_user_import_employee_type_add_child_{instance}{version}'
service_center_add_child_dag_id = f'michaelkorstna_uk_service_center_add_child_{instance}{version}'
timesheet_recalculation_child_dag_id = f'michaelkorstna_uk_user_import_timesheet_recalculation_child_{instance}{version}'
add_foreign_supervisor_child_dag_id = f'michaelkorstna_uk_user_import_add_foreign_supervisor_child_{instance}{version}'
add_supervisor_child_dag_id = f'michaelkorstna_uk_user_import_add_supervisor_child_{instance}{version}'
holiday_termination_proration_child_dag_id = f'michaelkorstna_uk_user_import_holiday_timeoff_type_termination_proration_assignment_child_{instance}{version}'
sick_leave_proration_child_dag_id = f'michaelkorstna_uk_user_import_timeoff_type_proration_assignment_child_{instance}{version}'
