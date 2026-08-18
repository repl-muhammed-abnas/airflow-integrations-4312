from tsystems.project_team_assignment_v3.config import *

region = 'eu-central-1'
environment = "production"

# Connection configuration
instance = 'prod'
company_key = 'Tsystems'

replicon_conn_id = 'tsystems_replicon_repliconint.projectimport'
sftp_conn_id = 'sftp_tsystems_Replicon_Logs'

log_filepath = "/PROD/Project Team Assignment"

tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}"
alert_email = "{{ var.value.dagrun_failure_alert_email }}"

# API configuration
http_conn_id = f'spacegate_http_conn_{instance}'
token_var = f"tsystems_caiman_access_token_variable_{instance}"

# Project team assignment specific
create_event_endpoint = "/horizon/sse/v1/resource.reservation.create.event.v1/cb05e9629958a46c1aae57e8886eca717c4b6073"
update_event_endpoint = "/horizon/sse/v1/resource.reservation.update.event.v1/d583e10be6155088450f971adfefbe992a9a843b"
search_capacity_endpoint = "/tsi/tsi-tmf716-resource-reservation/v1/capacity/search/id"

version = "v3"

master_dag_id = f"project_team_assignment_master_{instance}_{version}"
process_each_event_data_dag_id = f"project_team_assignment_process_each_event_data_child_{instance}_{version}"
individual_allocation_per_day_dag_id = f"project_team_assignment_individual_allocation_per_day_child_{instance}_{version}"
process_log_generation_dagid = f"project_team_assignment_log_generation_child_{instance}_{version}"

can_run_batch_task_var_name = f'project_team_assignment_{instance}_can_run_batch_task'

assignment_id_blob_key_name = f'project_team_assignment_id'

schedule_interval = "0/15 * * * *"

child_max_active_runs = 5
