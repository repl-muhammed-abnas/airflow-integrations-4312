# pylint: disable=wildcard-import unused-wildcard-import
from neology.user_import.config import *
from neology.user_import.mappers.employee_fields import required_employee_fields_list
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'neologyinc'

bamboohr_domain = 'neology'
bamboohr_conn_id = 'neology_bamboohr'
replicon_conn_id = 'neologyinc_replicon_repliconint.userimport'
sumo_conn_id = 'sumologic-dagrunlogger'

master_dagid = f'neology_user_import_master_{instance}'
create_user_child_dagid = f'neology_user_import_create_user_child_{instance}'
update_user_child_dagid = f'neology_user_import_update_user_child_{instance}'
process_user_child_dagid = f'neology_user_import_process_each_user_child_{instance}'
create_costcenters_child_dag_id = f'neology_user_import_process_create_costcenters_child_{instance}'
create_departments_child_dag_id = f'neology_user_import_process_create_departments_child_{instance}'
create_divisions_child_dag_id = f'neology_user_import_process_create_divisions_child_{instance}'
create_employeetypes_child_dag_id = f'neology_user_import_process_create_employee_types_child_{instance}'
create_locations_child_dag_id = f'neology_user_import_process_create_locations_child_{instance}'
create_servicecenters_child_dag_id = f'neology_user_import_process_create_service_centers_child_{instance}'
create_project_roles_child_dag_id = f'neology_user_import_process_create_project_roles_child_{instance}'
create_oef_tags_child_dag_id = f'neology_user_import_process_create_oef_tags_child_{instance}'
supervisor_assignment_child_dag_id = f'neology_user_import_supervisor_assignment_child_{instance}'

change_users_status_master_dag_id = f'neology_user_import_change_users_status_master_{instance}'
disable_users_child_dag_id = f'neology_user_import_disable_users_child_{instance}'
enable_users_child_dag_id = f'neology_user_import_enable_users_child_{instance}'

can_run_batch_task_var_name = f'neology_bamboohr_user_import_can_run_batch_task_{instance}'
last_synctime = f'neology_bamboohr_user_import_last_synctime_{instance}'

tenant_email = 'rsotelo@neology.mx,eangeles@neology.mx,sgoodbody@neology.com,enterpriseapplications@neology.net'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

replicon_default_password = f"neologyinc_replicon_default_password_{instance}"
required_employee_fields = required_employee_fields_list
