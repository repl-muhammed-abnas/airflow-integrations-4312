# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.workday_user_sync.user_schedule_v3.config import *

instance = "uat"
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'

tenant_email = 'utpal.chakraborty@vialto.com,hemanth.maru@vialto.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = "/Workday/Work Schedules/Test/Input"
archive_filepath = "/Workday/Work Schedules/Test/Archive"
log_filepath = "/Workday/Work Schedules/Test/Log"

dag_id_postfix = f'{instance}_v3'

can_run_batch_task_var_name = "can_run_batch_task_var_name"

main_dag = f'vialtopartners_user_schedule_import_master_{dag_id_postfix}'
process_user_schedule_child_dag = f'vialtopartners_user_schedule_import_process_each_user_schedule_child_dag_{dag_id_postfix}'
process_user_correction_child_dag = f'vialtopartners_user_schedule_import_schedule_correction_child_dag_{dag_id_postfix}'
process_new_schedule_creation = f'vialtopartners_user_schedule_import_new_schedule_creation_child_dag_{dag_id_postfix}'


disabled=True
