# pylint: disable=wildcard-import unused-wildcard-import
from momentive.payroll_export_file_transfer.mapper.payroll_details_mapper import momentive_payroll_export_mapper
from momentive.payroll_export_file_transfer.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'MomentiveTrial01'
replicon_conn_id = 'replicon_momentive_trial'

sftp_conn_id = 'rsftp-useast_for_testing'
secondary_sftp_conn_id = 'rsftp-useast_for_testing'

payroll_names = list({x['payroll_name'] for x in momentive_payroll_export_mapper})
disabled = True
