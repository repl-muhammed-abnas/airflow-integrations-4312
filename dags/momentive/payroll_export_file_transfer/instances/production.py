# pylint: disable=wildcard-import unused-wildcard-import
from momentive.payroll_export_file_transfer.mapper.payroll_details_mapper import momentive_payroll_export_mapper
from momentive.payroll_export_file_transfer.config import *

instance = 'prod'
region = 'us-east-1'
environment = 'production'

company_key = 'momentive'
replicon_conn_id = 'momentive-replicon-admin'

sftp_conn_id = 'sftp_momentive_MPMZS'
secondary_sftp_conn_id = 'sftp_momentive_542902'

payroll_names = list({x['payroll_name'] for x in momentive_payroll_export_mapper})
