# pylint: disable=wildcard-import unused-wildcard-import
from fujifilmdimatixg3.pto_timeoff_balance_update.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'fujifilmdimatixg3afmig'

replicon_conn_id = 'fujifilmdimatixg3afmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'

input_filepath = "/Fujifilm/fujifilmptoupdate/Input"
fromaddress_filepath = "/Fujifilm/fujifilmptoupdate/fromaddress"
archive_filepath = "/Fujifilm/fujifilmptoupdate/Archive"


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
cc_email = '{{ var.value.dagrun_internal_testing_email }}'

max_active_runs_child = 4
disabled = True
