# pylint: disable=wildcard-import unused-wildcard-import
from pimco.project_import.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'PIMCO'
replicon_conn_id = 'pimco-replicon-production'
sftp_conn_id = 'sftp_pimco_537254'

fund_input_filepath = '/Project Import/Input/Funds/'
fund_processing_filepath = '/Project Import/Processing/Funds/'
fund_log_filepath = '/Project Import/Logs/'

deal_input_filepath = '/Project Import/Input/Deals/'
deal_processing_filepath = '/Project Import/Processing/Deals/'
deal_log_filepath = '/Project Import/Logs/'

entity_input_filepath = '/Project Import/Input/Entity/'
entity_processing_filepath = '/Project Import/Processing/Entity/'
entity_log_filepath = '/Project Import/Logs/'

consultant_deal_input_filepath = '/Project Import/Input/Deals_CON/'
consultant_deal_processing_filepath = '/Project Import/Processing/Deals/'
consultant_deal_log_filepath = '/Project Import/Logs/'

consultant_fund_input_filepath = '/Project Import/Input/Funds_CON/'
consultant_fund_processing_filepath = '/Project Import/Processing/Funds/'
consultant_fund_log_filepath = '/Project Import/Logs/'

consultant_entity_input_filepath = '/Project Import/Input/Entity_CON/'
consultant_entity_processing_filepath = '/Project Import/Processing/Entity/'
consultant_entity_log_filepath = '/Project Import/Logs/'

archieve_input_filepath = '/Project Import/Archieve/'

base_project_name = "PIMCO Model Task"
consultant_base_project_name = "Consultant Model Task"

tenant_email = 'Mayank.Sharma@pimco.com,Shekhar.Gupta@pimco.com,s_repnotif@pimco.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
