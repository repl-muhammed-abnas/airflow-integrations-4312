from tsystems.project_team_assignment_v2.config import *

region = 'eu-central-1'
environment = "pre-production"

# Connection configuration
instance = 'trial'
company_key = 'TsystemsSB'

replicon_conn_id = 'tsystems_replicon_replicon.admin'
sftp_conn_id = 'sftp_useast2'

log_filepath = "/TsystemsSB/project_assignment/Replicon_TARDIS_API/LOGS"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

# API configuration
http_conn_id = f'spacegate_http_conn_{instance}'
token_var = f"tsystems_caiman_access_token_variable_{instance}"

# Project team assignment specific
create_event_endpoint = "/horizon/sse/v1/resource.reservation.create.event.v1/6ad0febdfbf34b1b3beee6d73eaa1b9c67852874"
update_event_endpoint = "/horizon/sse/v1/resource.reservation.update.event.v1/df79237d56a08ed919f9330ee3bdd71eee64121e"
search_capacity_endpoint = "/tsi/tsi-tmf716-resource-reservation/v1/capacity/search/id"

version = "v2"

master_dag_id = f"project_team_assignment_master_{instance}_{version}"
process_each_event_data_dag_id = f"project_team_assignment_process_each_event_data_child_{instance}_{version}"
individual_allocation_per_day_dag_id = f"project_team_assignment_individual_allocation_per_day_child_{instance}_{version}"
process_log_generation_dagid = f"project_team_assignment_log_generation_child_{instance}_{version}"

can_run_batch_task_var_name = f'project_team_assignment_{instance}_can_run_batch_task'

assignment_id_blob_key_name = f'project_team_assignment_id'

disabled=True
