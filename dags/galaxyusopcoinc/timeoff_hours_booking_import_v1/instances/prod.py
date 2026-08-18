# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.timeoff_hours_booking_import_v1.config import *

instance = 'prod'
environment = 'production'

company_key = 'GalaxyUSOpcoInc'
replicon_conn_id = 'galaxyusopcoinc_replicon_admin'
pgp_conn_id = 'pgp_vialto_partners'

sftp_conn_id = "sftp_galaxyusopcoinc_676273"

input_filepath = '/Workday/WD TO Inbound/Prod/Input'
log_filepath = '/Workday/WD TO Inbound/Prod/Log'
archive_filepath = '/Workday/WD TO Inbound/Prod/Archive'

tenant_email = 'gbl_vialto_tech_digital_workday@vialto.com,hemanth.maru@vialto.com,utpal.chakraborty@vialto.com,farhan.afzal@vialto.com,atul.singh@vialto.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'vialto_timeoff_sync_{instance}_can_run_batch_task'
can_decrypt_file = f'vialto_timeoff_sync_{instance}_can_decrypt_file'

master_dag_id = f"vialto_timeoff_booking_import_master_{instance}_v1"
process_each_user_dag_id = f"vialto_timeoff_booking_import_process_each_user_child_{instance}_v1"
process_each_timeoff_booking = f"vialto_timeoff_booking_import_process_each_timeoff_booking_child_{instance}_v1"
