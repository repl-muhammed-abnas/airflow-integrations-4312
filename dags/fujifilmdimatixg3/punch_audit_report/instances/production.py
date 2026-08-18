# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from fujifilmdimatixg3.punch_audit_report.config import *

instance = "production"
environment = 'production'
company_key = 'FUJIFILMDimatixG3'

tenant_email = "amclaughlin.contractor@fujifilm.com,fdmx-prodleads@fujifilm.com,madison.simoneau@fujifilm.com,pfountain@fujifilm.com,khebert@fujifilm.com,jpushee@fujifilm.com,fdmxbenefits@fujifilm.com"
bcc_tenant_email =  '{{ var.value.dagrun_internal_log_email }}'

replicon_conn_id = 'FUJIFILMDimatixG3_replicon_admin'
sftp_conn_id = 'sftp_fujifilmdimatixg3_41694'

punch_audit_report_filepath = "/punch_audit_report"

department_details_var_name = f'fujifilmdimatix_{instance}_department_details'
default_department_list_var = f'fujifilmdimatix_project_{instance}_default_department_list'
