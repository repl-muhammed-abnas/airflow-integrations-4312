# pylint: disable=wildcard-import unused-wildcard-import
from vjtechnologies.projectsync_v1.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'VJTechnologiestrial01'
replicon_conn_id = 'VJTechnologiestrial01_replicon_admin'
time_zone = 'America/New_York'

entity_name = 'vjs'

max_active_runs_child = 5

tenant_email = "kventura@vjt.com, axsupport@vjt.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'VJTechnologiestrial01_sftp_RepliconSFTP'

input_filepath = '/Replicon UAT/Project Sync/VJS/Input'
reference_filepath = '/Replicon UAT/Project Sync/VJS/Reference/'
log_filepath = '/Replicon UAT/Project Sync/VJS/Logs/'
archive_filepath = '/Replicon UAT/Project Sync/VJS/Archive/'

schedule_interval = '30 0 * * *'
can_run_batch_task = f'vjtechnologies_projectsync_vjs_can_run_batch_task_{instance}'

add_task_child_dagid = f'vjtechnologies_{entity_name}_add_task_child_{instance}'
client_project_import_master_dagid = f'vjtechnologies_{entity_name}_projectsync_client_project_import_master_{instance}'
process_each_client_record_child_dagid = f'vjtechnologies_{entity_name}_process_each_client_record_child_{instance}'
process_each_file_dagid = f'vjtechnologies_{entity_name}_projectsync_client_project_import_process_each_file_child_{instance}'
process_each_project_record_child_dagid = f'vjtechnologies_{entity_name}_process_each_project_record_child_{instance}'
update_task_child_dagid = f'vjtechnologies_{entity_name}_update_task_child_{instance}'

disabled=True
