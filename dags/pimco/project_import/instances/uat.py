# pylint: disable=wildcard-import unused-wildcard-import
from pimco.project_import.config import *

instance = 'uat'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'PIMCOTrial02'
replicon_conn_id = 'pimcotrial02-admin'
sftp_conn_id = 'sftp_pimcotrial02_537254'

fund_input_filepath = '/Project Import/Trial/Funds/'
fund_processing_filepath = '/Project Import/Trial/Processing/Funds/'
fund_log_filepath = '/Project Import/Trial/Logs/'

deal_input_filepath = '/Project Import/Trial/Deals/'
deal_processing_filepath = '/Project Import/Trial/Processing/Deals/'
deal_log_filepath = '/Project Import/Trial/Logs/'

entity_input_filepath = '/Project Import/Trial/Entity/'
entity_processing_filepath = '/Project Import/Trial/Processing/Entity/'
entity_log_filepath = '/Project Import/Trial/Logs/'

consultant_deal_input_filepath = '/Project Import/Trial/Deals_CON/'
consultant_deal_processing_filepath = '/Project Import/Trial/Processing/Deals/'
consultant_deal_log_filepath = '/Project Import/Trial/Logs/'

consultant_fund_input_filepath = '/Project Import/Trial/Funds_CON/'
consultant_fund_processing_filepath = '/Project Import/Trial/Processing/Funds/'
consultant_fund_log_filepath = '/Project Import/Trial/Logs/'

consultant_entity_input_filepath = '/Project Import/Trial/Entity_CON/'
consultant_entity_processing_filepath = '/Project Import/Trial/Processing/Entity/'
consultant_entity_log_filepath = '/Project Import/Trial/Logs/'

archieve_input_filepath = '/Project Import/Trial/Archieve/'

base_project_name = "PIMCO Model Task"
consultant_base_project_name = "Consultant Model Task"

tenant_email = "Mayank.Sharma@pimco.com,Shekhar.Gupta@pimco.com,s_repnotif@pimco.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
