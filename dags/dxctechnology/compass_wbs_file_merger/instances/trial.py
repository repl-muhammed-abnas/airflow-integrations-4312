# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_file_merger.config import *

instance = 'trial'
region = 'us-east-2'
environment = 'pre-production'
sftp_conn_id = "dxcafmig_replicon_sftp"
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
sub_erp = 'trial'
dag_id_postfix = f'{instance}_{sub_erp}'
