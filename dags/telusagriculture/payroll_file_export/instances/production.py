# pylint: disable=wildcard-import unused-wildcard-import
from telusagriculture.payroll_file_export.config import *

instance = 'prod'
environment = 'production'

company_key = 'telusagriculture'
replicon_conn_id = 'telusagriculture-replicon-admin'

input_filepath = '/Production'
output_filepath = '/incoming/REPLICON'
archive_filepath = '/Archive'

sftp_conn_id = 'sftp_telusagriculture_replicon_670252'
secondary_sftp_conn_id = 'sftp_telusagriculture_client_22849339P'
