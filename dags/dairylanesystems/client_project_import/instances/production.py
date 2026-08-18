# pylint: disable=wildcard-import unused-wildcard-import
from dairylanesystems.client_project_import.config import *

instance = "production"
environment = 'production'
company_key = 'dairylanesystems'
replicon_conn_id = 'dairylanesystems_replicon_John.VanLogtenstein'
sftp_conn_id = "sftp_dairylanesystems_639463"

tenant_email = "timesheets@dairylane.ca"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/Production/Input'
input_filepath_reference_file = '/Production/Reference/import_reference.csv'
archive_filepath = '/Production/Archive/'
log_filepath = '/Production/Logs/logs_'


can_run_batch_task_child = f'dairy_lane_client_project_import_child_{instance}_can_run_batch_task'
address_oef_name='Address/Location'
clientname_oef='Client Name'
