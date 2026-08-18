# pylint: disable=wildcard-import unused-wildcard-import
from omd.singapore_user_import.config import *
instance = 'trial'
environment = 'production'
company_key = 'OMDSingaporePteLtd'
replicon_conn_id = 'OMDSingaporePteLtd_replicon_admin'

tenant_email = 'omg-sg-repliconsupport@omnicommediagroup.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_OMDSingaporePteLtd_660053'

input_filepath = '/User Import/Input'
reference_filepath = '/User Import/Reference/Userimportreference.csv'
archive_filepath = '/User Import/Archive/'

can_run_batch_child = f'omdsingaporepteltd_user_import_singapore_can_run_batch_task_{instance}'
