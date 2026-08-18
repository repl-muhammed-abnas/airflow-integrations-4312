# pylint: disable=wildcard-import unused-wildcard-import
from omd.singapore_user_import.config import *
instance = 'trial'
environment = 'pre-production'
company_key = 'OMDSingaporePteLtdafmig'
replicon_conn_id = 'omdsingaporepteltdafmig_replicon_admin'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'

input_filepath = '/OMD/Singapore/Input'
reference_filepath = '/OMD/Singapore/Reference/Userimportreference.csv'
archive_filepath = '/OMD/Singapore/Archive/'

can_run_batch_child = f'omdsingaporepteltd_user_import_singapore_can_run_batch_task_{instance}'

disabled=True
