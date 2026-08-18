# pylint: disable=wildcard-import unused-wildcard-import
from vjtechnologies.projectsync_v1.config import *
region = 'us-east-1'
instance = 'prod'
environment = 'production'
company_key = 'VJTechnologies'
replicon_conn_id = 'VJTechnologies_replicon_admin'
time_zone = 'America/New_York'

entity_name = 'vjct'

max_active_runs_child = 5

tenant_email = "kventura@vjt.com, axsupport@vjt.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'VJTechnologies_sftp_RepliconSFTP'

input_filepath = '/Replicon/Project Sync/VJCT/Input'
reference_filepath = '/Replicon/Project Sync/VJCT/Reference/'
log_filepath = '/Replicon/Project Sync/VJCT/Logs/'
archive_filepath = '/Replicon/Project Sync/VJCT/Archive/'

schedule_interval = '30 0 * * *'
can_run_batch_task = f'vjtechnologies_projectsync_vjct_can_run_batch_task_{instance}'


add_task_child_dagid = f'vjtechnologies_{entity_name}_add_task_child_{instance}'
client_project_import_master_dagid = f'vjtechnologies_{entity_name}_projectsync_client_project_import_master_{instance}'
process_each_client_record_child_dagid = f'vjtechnologies_{entity_name}_process_each_client_record_child_{instance}'
process_each_file_dagid = f'vjtechnologies_{entity_name}_projectsync_client_project_import_process_each_file_child_{instance}'
process_each_project_record_child_dagid = f'vjtechnologies_{entity_name}_process_each_project_record_child_{instance}'
update_task_child_dagid = f'vjtechnologies_{entity_name}_update_task_child_{instance}'
