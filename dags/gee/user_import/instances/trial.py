# pylint: disable=wildcard-import unused-wildcard-import
from gee.user_import.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'geeafmig'
replicon_conn_id = 'gee_replicon_admin'
sftp_conn_id = "sftp_internal"
execution_timeout_days = 14
child_dag_max_active_runs = 2

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

input_filepath = "/gee/user_import/trial/input"
archive_filepath = "/gee/user_import/trial/archive"
reference_filepath = "/gee/user_import/trial/reference"
usersync_filepath = "/gee/user_import/trial/usersync"

master_dag_id = f'gee_user_import_master_{instance}'
create_user_child = f'gee_create_user_child_{instance}'
create_supervisor_child = f'gee_create_supervisor_child_{instance}'
gee_supervisor_assignment_child = f'gee_supervisor_assignment_child_{instance}'
update_user_child = f'gee_update_user_child_{instance}'
disable_user_child = f'gee_disable_user_child_{instance}'
gee_usersync_sendlog = f'gee_usersync_sendlog_{instance}'

disabled=True
