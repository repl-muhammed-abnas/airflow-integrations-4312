# pylint: disable=wildcard-import unused-wildcard-import
from crl.report_to_sftp_v2.config import *

instance = 'trial'

company_key = 'CharlesRiverLaboratoriesSandbox'
replicon_conn_id = 'charlesriverlaboratoriessandbox_repliconint_timeexport'
sftp_conn_id = "sftp_useast2"

extract_report_file_path="/hahnemann/input"
archive_filepath="/hahnemann/archive"

version = 'v2'

project_master = f"crl_report_to_sftp_project_master_{instance}_{version}"
project_master_uk = f"crl_report_to_sftp_project_master_uk_{instance}_{version}"
project_master_germany = f"crl_report_to_sftp_project_master_germany_{instance}_{version}"
project_child = f"crl_report_to_sftp_project_child_{instance}_{version}"
user_master = f"crl_report_to_sftp_user_master_{instance}_{version}"
user_master_ireland = f"crl_report_to_sftp_user_master_ireland_{instance}_{version}"
user_master_israel = f"crl_report_to_sftp_user_master_israel_{instance}_{version}"
user_master_switzerland = f"crl_report_to_sftp_user_master_switzerland_{instance}_{version}"
user_master_uk = f"crl_report_to_sftp_user_master_uk_{instance}_{version}"
user_master_brazil = f"crl_report_to_sftp_user_master_brazil_{instance}_{version}"
user_master_germany = f"crl_report_to_sftp_user_master_germany_{instance}_{version}"
user_child = f"crl_report_to_sftp_user_child_{instance}_{version}"

dm_emp_new_master_dag_id = f"crl_report_to_sftp_dm_emp_new_master_{instance}_{version}"
network_file_draft_master_dag_id = f"crl_report_to_sftp_network_file_draft_master_{instance}_{version}"
