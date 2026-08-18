# pylint: disable=wildcard-import unused-wildcard-import
from datetime import timedelta
from ge.user_sync_poland.config import *
from ge.user_sync_poland.mapper.poland_master_mapper import ge_poland_user_sync_master_mapper

instance = 'trial'

environment = 'pre-production'
company_key = 'geafmig'

replicon_conn_id = 'geafmig_replicon_admin'
sftp_conn_id = 'sftp_internal_useast2'
sftp_ge_internal = 'sftp_internal_eucentral'
pgp_conn_id = 'GEafmig_User_Import_PGP'

master_dag_interval = timedelta(seconds=60)

sftp_input_filepath = "/GE/poland/user_import/input"
sftp_archive_filepath = "/GE/poland/user_import/archive"
sftp_log_filepath = "/GE/poland/user_import/logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

POLAND_MASTER_MAPPER = ge_poland_user_sync_master_mapper

can_run_batch_task_var_name = f"ge_poland_user_import_can_run_batch_task_{instance}"

master_dag_id = f'ge_poland_user_import_master_{instance}'

child_schedule_add_dag_id = f'ge_poland_user_import_schedule_add_child_{instance}'
sub_child_schedule_add_dag_id = f'ge_poland_user_import_schedule_add_sub_child_{instance}'
child_suspend_assignment_category_custom_field_dag_id = f'ge_poland_user_import_suspend_assignment_category_custom_field_child_{instance}'
child_legacy_payroll_id_servicecenter_add_dag_id = f'ge_poland_user_import_legacy_payroll_id_servicecenter_add_child_{instance}'
sub_child_legacy_payroll_id_servicecenter_add_dag_id = f'ge_poland_user_import_legacy_payroll_id_servicecenter_add_sub_child_{instance}'

child_process_each_user_dag_id = f'ge_poland_user_import_process_each_user_child_{instance}'
child_add_foreign_supervisor_dag_id = f'ge_poland_user_import_add_foreign_supervisor_child_{instance}'
child_add_supervisor_dag_id = f'ge_poland_user_import_add_supervisor_child_{instance}'

child_update_user_dag_id = f'ge_poland_user_import_update_user_child_{instance}'
child_add_user_dag_id = f'ge_poland_user_import_add_user_child_{instance}'

child_add_update_timeoff_type_dag_id = f'ge_poland_user_import_workflow_to_add_update_timeoff_types_child_{instance}'

child_assign_timeoff_policy_annual_leave_on_termination_dag_id = f'ge_poland_user_import_assign_timeoff_policy_annual_leave_on_termination_child_{instance}'
child_assign_prorated_timeoff_policy_annual_leave_dag_id = f'ge_poland_user_import_assign_prorated_timeoff_policy_annual_leave_child_{instance}'

child_assign_timeoff_policy_compensatory_timeoff_dag_id = f'ge_poland_user_import_assign_timeoff_policy_compensatory_timeoff_child_{instance}'
