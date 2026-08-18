# pylint: disable=wildcard-import unused-wildcard-import
from crl.report_to_sftp.config import *

instance = 'uat'

company_key = 'CharlesRiverLaboratoriesSandbox'
replicon_conn_id = 'charlesriverlaboratoriessandbox_repliconint_timeexport'
sftp_conn_id = "sftp_crl_603355"

extract_report_file_path="/Test/Outbound/Reconcile/Input"

project_master = f"crl_report_to_sftp_project_master_{instance}"
project_child = f"crl_report_to_sftp_project_child_{instance}"
user_master = f"crl_report_to_sftp_user_master_{instance}"
user_child = f"crl_report_to_sftp_user_child_{instance}"

disabled=True
