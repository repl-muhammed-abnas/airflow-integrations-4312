"""
UAT instance configuration for Unisys Time Export to Oracle Integration
"""
# pylint: disable=wildcard-import unused-wildcard-import
from unisys.time_export_to_oracle.config import *

# Instance identification
instance = 'uat'
environment = "pre-production"
company_key = 'UnisysUAT'
replicon_conn_id = 'unisysuat_replicon_repliconint'
pgp_conn_id = 'pgp_unisys_time_export_oracle_uat'
secondary_pgp_conn_id = 'pgp_unisys_time_export_oracle_secondary_uat'

# SFTP configuration
sftp_conn_id = 'sftp_unisysuat_710319_UAT'
export_csv_filepath = '/Outbound/Time Export'
secondary_export_csv_filepath = '/Outbound/Time Export_Ops'

export_file_prefix = 'UAT_TimeExport'

# Email configuration
tenant_email = 'Prashant.Vishwakarma@unisys.com,Raviraj.Ramachandra@in.unisys.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},Unisysproject@deltek.com'

# DAG identifiers
master_dag = f'unisys_oracle_time_export_master_{instance}'

disabled=True
