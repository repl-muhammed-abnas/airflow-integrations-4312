# pylint: disable=wildcard-import unused-wildcard-import
from fujifilmdimatixg3.punch_audit_report.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'FUJIFILMDimatixG3afmig'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

replicon_conn_id = 'fujifilmdimatixg3afmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'

punch_audit_report_filepath = "/fujifilmdimatixg3/punch_audit_report"

department_details_var_name = f'fujifilmdimatix_project_{instance}_department_details'
default_department_list_var = f'fujifilmdimatix_project_{instance}_default_department_list'
