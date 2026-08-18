# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_off_balance_report_to_sftp.config import *

instance = 'uat'

company_key = 'CharlesRiverLaboratoriesSandbox'
replicon_conn_id = 'charlesriverlaboratoriessandbox_repliconint_timeexport'
sftp_conn_id = "sftp_crl_603355"

extract_report_file_path="/Test/Outbound/Time off Report"

user_master_usa = f"crl_time_off_balance_report_to_sftp_master_usa_{instance}"
user_master_can = f"crl_time_off_balance_report_to_sftp_master_can_{instance}"
user_child = f"crl_time_off_balance_report_to_sftp_child_{instance}"

disabled=True
