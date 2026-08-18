# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.workday_user_sync.user_schedule_v4.config import *

instance = "production"
environment = "production"

company_key = 'GalaxyUSOpcoInc'
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'
replicon_conn_id = "galaxyusopcoinc_replicon_admin"

tenant_email = 'gbl_vialto_technology_digital_replicon_time_entry@vialto.com,utpal.chakraborty@vialto.com,hemanth.maru@vialto.com,farhan.afzal@vialto.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = "/Workday/Work Schedules/Prod/Input"
archive_filepath = "/Workday/Work Schedules/Prod/Archive"
log_filepath = "/Workday/Work Schedules/Prod/Log"

dag_id_postfix = f'{instance}_v4'

can_run_batch_task_var_name = "can_run_batch_task_var_name"

main_dag = f'vialtopartners_user_schedule_import_master_{dag_id_postfix}'
process_user_schedule_child_dag = f'vialtopartners_user_schedule_import_process_each_user_schedule_child_dag_{dag_id_postfix}'
process_new_schedule_creation = f'vialtopartners_user_schedule_import_new_schedule_creation_child_dag_{dag_id_postfix}'

can_decrypt_file = f"vialtopartners_user_schedule_sync_decrypt_file_{dag_id_postfix}"
