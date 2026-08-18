"""
Development instance configuration for Unisys Time Export to Oracle Integration
"""
# pylint: disable=wildcard-import unused-wildcard-import
from unisys.time_export_to_oracle.config import *

# Instance identification
instance = 'dev'
environment = "pre-production"
company_key = 'Unisysdev'
replicon_conn_id = 'unisysdev_replicon_repliconint'
pgp_conn_id = 'pgp_unisys_time_export_oracle_dev'
secondary_pgp_conn_id = 'pgp_unisys_time_export_oracle_secondary_dev'

# SFTP configuration
sftp_conn_id = 'sftp_unisysdev_710319'
export_csv_filepath = '/Outbound/Time Export'
secondary_export_csv_filepath = '/Outbound/Time Export_Ops'

export_file_prefix = 'DEV_TimeExport'

# Email configuration
tenant_email = 'Prashant.Vishwakarma@unisys.com,Raviraj.Ramachandra@in.unisys.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},Unisysproject@deltek.com'

# DAG identifiers
master_dag = f'unisys_oracle_time_export_master_{instance}'

disabled=True
