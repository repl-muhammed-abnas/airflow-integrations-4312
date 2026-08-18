# pylint: disable=wildcard-import unused-wildcard-import
from pimco.project_import.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'PIMCOTrial02'
replicon_conn_id = 'pimcotrial02-admin'
sftp_conn_id = 'sftp_internal'

fund_input_filepath = '/PIMCOTrial01/ProjectImport/Fund/Input/'
fund_processing_filepath = '/PIMCOTrial01/ProjectImport/Fund/Archieve/'
fund_log_filepath = '/PIMCOTrial01/ProjectImport/Fund/Log/'

deal_input_filepath = '/PIMCOTrial01/ProjectImport/Deal/Input'
deal_processing_filepath = '/PIMCOTrial01/ProjectImport/Deal/Archieve/'
deal_log_filepath = '/PIMCOTrial01/ProjectImport/Deal/Log/'

entity_input_filepath = '/PIMCOTrial01/ProjectImport/Entity/Input/'
entity_processing_filepath = '/PIMCOTrial01/ProjectImport/Entity/Archieve/'
entity_log_filepath = '/PIMCOTrial01/ProjectImport/Entity/Log'

consultant_deal_input_filepath = '/PIMCOTrial01/ProjectImport/CON_Deal/Input/'
consultant_deal_processing_filepath = '/PIMCOTrial01/ProjectImport/CON_Deal/Archieve/'
consultant_deal_log_filepath = '/PIMCOTrial01/ProjectImport/CON_Deal/Log/'

consultant_fund_input_filepath = '/PIMCOTrial01/ProjectImport/CON_Fund/Input/'
consultant_fund_processing_filepath = '/PIMCOTrial01/ProjectImport/CON_Fund/Archieve/'
consultant_fund_log_filepath = '/PIMCOTrial01/ProjectImport/CON_Fund/Log/'

consultant_entity_input_filepath = '/PIMCOTrial01/ProjectImport/CON_Entity/Input/'
consultant_entity_processing_filepath = '/PIMCOTrial01/ProjectImport/CON_Entity/Archieve/'
consultant_entity_log_filepath = '/PIMCOTrial01/ProjectImport/CON_Entity/Log/'

archieve_input_filepath = '/PIMCOTrial01/ProjectImport/Archieve/'

base_project_name = "PIMCO Model Task"
consultant_base_project_name = "Consultant Model Task"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
