# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.user_import_v1.config import *
from bearingpoint.user_import_v1.mappers.location_wise_details import location_wise_data

instance = "trial"
version = "_v1"

region = 'eu-central-1'
environment = "pre-production"

company_key = "BearingPointSandbox"

replicon_conn_id = "bearingpointsandbox_replicon_repliconint_user_import"
http_conn_id = f"bearingpoint_user_import_http_logs_api_{instance}"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f"bearingpoint_user_import_process_users_can_run_batch_task_{instance}{version}"
process_payload_child_dag_id = f"bearingpoint_user_import_process_payload_child_{instance}{version}"
process_user_record_child_dag_id = f"bearingpoint_user_import_process_user_record_child_{instance}{version}"
add_user_child_dag_id = f"bearingpoint_user_import_process_add_user_child_{instance}{version}"
update_user_child_dag_id = f"bearingpoint_user_import_process_update_user_child_{instance}{version}"

create_locations_child_dag_id = f"bearingpoint_user_import_create_locations_child_{instance}{version}"
create_costcenters_child_dag_id = f"bearingpoint_user_import_create_costcenters_child_{instance}{version}"
create_departments_child_dag_id = f"bearingpoint_user_import_create_departments_child_{instance}{version}"
create_employeetypes_child_dag_id = f"bearingpoint_user_import_create_employeetypes_child_{instance}{version}"
create_servicecenters_child_dag_id = f"bearingpoint_user_import_create_servicecenters_child_{instance}{version}"

location_wise_data_mapper = location_wise_data

token_var = f"bearingpoint_token_variable_{instance}"
