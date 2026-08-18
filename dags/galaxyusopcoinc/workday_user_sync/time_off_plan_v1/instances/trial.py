# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.workday_user_sync.time_off_plan_v1.config import *

instance = "trial"
sftp_conn_id = 'sftp_useast2'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
input_filepath = "/Workday/Time off Plan/Sandbox/Input"
archive_filepath = "/Workday/Time off Plan/Sandbox/Archive"
log_filepath = "/Workday/Time off Plan/Sandbox/Log"
disabled = True


# dag_id's
master_dag_id = f'vialtopartners_time_off_plan_master_{instance}_v1'
create_timeoff_type_dag_id = f'vialtopartners_timeoff_type_create_child_{instance}_v1'
update_user_timeoff_dag_id = f'vialtopartners_each_user_time_off_type_child_{instance}_v1'
update_generic_key_value_mapper_dag_id = f'vialtopartners_timeoff_plan_update_user_sync_mapper_child_{instance}_v1'
disable_timeoff_type_dag_id = f'vialtopartners_timeoff_type_disable_child_{instance}_v1'
enable_timeoff_type_dag_id = f'vialtopartners_timeoff_type_enable_child_{instance}_v1'
update_timeoff_type_dag_id = f'vialtopartners_timeoff_type_update_child_{instance}_v1'
