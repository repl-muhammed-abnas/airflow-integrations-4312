# pylint: disable=wildcard-import unused-wildcard-import
from dairylanesystems.client_project_import.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'dairylanesystemsafmig'
replicon_conn_id = 'dairylanesystemsafmig_replicon_johnvanlogtenstein'
sftp_conn_id = "sftp_useast2"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/DLS/Input'
input_filepath_reference_file = '/DLS/Production/Reference/import_reference.csv'
archive_filepath = '/DLS/Production/Archive/'
log_filepath = '/DLS/Logs/logs_'


can_run_batch_task_child = f'dairy_lane_client_project_import_child_{instance}_can_run_batch_task'
address_oef_name='Address/Location'
clientname_oef='Client Name'

disable=True

disabled=True
