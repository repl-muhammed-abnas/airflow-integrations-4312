# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.user_import.config import *
from bearingpoint.user_import.mappers.location_wise_details import location_wise_data

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "BearingPointGmbH"

replicon_conn_id = "bearingpointgmbh_replicon_repliconint.user_import"
http_conn_id = f"bearingpoint_user_import_http_logs_api_{instance}"

tenant_email = "work.smtp-0125104@bearingpoint.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f"bearingpoint_user_import_process_users_can_run_batch_task_{instance}"
process_payload_child_dag_id = f"bearingpoint_user_import_process_payload_child_{instance}"
process_user_record_child_dag_id = f"bearingpoint_user_import_process_user_record_child_{instance}"
add_user_child_dag_id = f"bearingpoint_user_import_process_add_user_child_{instance}"
update_user_child_dag_id = f"bearingpoint_user_import_process_update_user_child_{instance}"

create_locations_child_dag_id = f"bearingpoint_user_import_create_locations_child_{instance}"
create_costcenters_child_dag_id = f"bearingpoint_user_import_create_costcenters_child_{instance}"
create_departments_child_dag_id = f"bearingpoint_user_import_create_departments_child_{instance}"
create_employeetypes_child_dag_id = f"bearingpoint_user_import_create_employeetypes_child_{instance}"
create_servicecenters_child_dag_id = f"bearingpoint_user_import_create_servicecenters_child_{instance}"

location_wise_data_mapper = location_wise_data

token_var = f"bearingpoint_token_variable_{instance}"
