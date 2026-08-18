# pylint: disable=wildcard-import unused-wildcard-import
from sasglobal.decryptfiles.config import *

instance = 'sandbox'
region = 'us-east-1'
environment = 'pre-production'

pgp_conn_id = 'pgp_sas_global'
sftp_conn_id = 'sasglobal_sftp_568340'
company_key = 'SASGlobalSB'
replicon_conn_id = 'sasglobalsb_replicon_replicon'

offering_input_filepath = '/Inbound/OEF/Offerings Supported'
offering_processing_filepath = '/Inbound/OEF/Offerings Supported/processing/'
offering_processing_archivepath = '/Inbound/OEF/archive/'

user_input_filepath = '/Inbound/User'
user_processing_filepath = '/Inbound/User/processing/'
user_archivepath = '/Inbound/User/archive/'

department_input_filepath = '/Inbound/Department'
department_processing_filepath = '/Inbound/Department/processing/'
department_archivepath = 'Inbound/Department/archive/'

geo_input_filepath = '/Inbound/OEF/GEO'
geo_processing_filepath = '/Inbound/OEF/GEO/processing/'
geo_archivepath = '/Inbound/OEF/archive/'
disabled = True
