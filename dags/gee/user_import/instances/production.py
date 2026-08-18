# pylint: disable=wildcard-import unused-wildcard-import
from gee.user_import.config import *

instance = "production"
environment = 'production'
company_key = 'GEE'
replicon_conn_id = 'gee_replicon_admin'
sftp_conn_id = "sftp_gee_561279"

tenant_email = "Beverley.Gleave@globaleagle.com"
bcc_tenant_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = "/Prod/input"
archive_filepath = "/Prod/Archive"
reference_filepath = "/Prod/Reference"
usersync_filepath = "/usersync"

master_dag_id = f'gee_user_import_master_{instance}'
create_user_child = f'gee_create_user_child_{instance}'
create_supervisor_child = f'gee_create_supervisor_child_{instance}'
gee_supervisor_assignment_child = f'gee_supervisor_assignment_child_{instance}'
update_user_child = f'gee_update_user_child_{instance}'
disable_user_child = f'gee_disable_user_child_{instance}'
gee_usersync_sendlog = f'gee_usersync_sendlog_{instance}'
