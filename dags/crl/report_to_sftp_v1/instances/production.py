# pylint: disable=wildcard-import unused-wildcard-import
from crl.report_to_sftp_v1.config import *

instance = 'production'
environment = 'production'

company_key = 'CharlesRiverLaboratories'
replicon_conn_id = 'charlesriverlaboratories_report_to_sftp_recon'
sftp_conn_id = "sftp_crl_603355"

extract_report_file_path="/Production/Outbound/Reconcile/Input"
archive_filepath="/Production/Outbound/Reconcile/Archive"

version = 'v1'

project_master = f"crl_report_to_sftp_project_master_{instance}_{version}"
project_child = f"crl_report_to_sftp_project_child_{instance}_{version}"
user_master = f"crl_report_to_sftp_user_master_{instance}_{version}"
user_child = f"crl_report_to_sftp_user_child_{instance}_{version}"

dm_emp_new_master_dag_id = f"crl_report_to_sftp_dm_emp_new_master_{instance}_{version}"
network_file_draft_master_dag_id = f"crl_report_to_sftp_network_file_draft_master_{instance}_{version}"
