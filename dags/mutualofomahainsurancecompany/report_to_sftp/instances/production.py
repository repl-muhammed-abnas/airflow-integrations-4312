# pylint: disable=wildcard-import unused-wildcard-import
from mutualofomahainsurancecompany.report_to_sftp.config import *

instance = 'production'
environment = 'production'

company_key = 'MutualofOmahaInsuranceCompany'
replicon_conn_id = 'mutualofomahainsurancecompany_replicon_adminr'

extract_report_file_path="/incoming/prod/"

workplace_solution_sftp_conn_id = 'mutualofomahainsurancecompany_workplace_solution_sftp'
iwp_sftp_conn_id = 'mutualofomahainsurancecompany_iwp_sftp'
sr_health_sftp_conn_id = 'mutualofomahainsurancecompany_sr_health_sftp'
