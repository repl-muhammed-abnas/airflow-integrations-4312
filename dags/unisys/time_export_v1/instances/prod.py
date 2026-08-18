"""
Development instance configuration for Unisys Fieldglass Time Export Integration
Contains only the configurations that are actually used in the integration

Based on design document: Replicon to Fieldglass Integration - Technical Specification V1.1
"""
# pylint: disable=wildcard-import unused-wildcard-import
from unisys.time_export_v1.config import *

region = "us-east-1"
environment = "production"

# Instance identification
instance = 'production'
company_key = 'Unisyscorporation'  # As per design doc
replicon_conn_id = 'unisyscorporation_replicon.repliconint'
pgp_conn_id = 'pgp_unisys_time_export_oracle_prod'
secondary_pgp_conn_id = 'pgp_unisys_time_export_oracle_secondary_prod'

# SFTP configuration based on design doc
# Host: rsftp-useast.replicon.com
# Username: 710319
# Sandbox path: /Sandbox/Outbound/Fieldglass
sftp_conn_id = 'sftp_unisyscorporation_710319_prod'
export_csv_filepath = '/Outbound/Fieldglass'
export_base_report_filepath = '/Outbound/Fieldglass/Logs'
export_logs_csv_filepath = '/Outbound/Fieldglass/Logs'
export_csv_to_secondary_filepath = '/Outbound/Fieldglass_Ops'

# Email configuration
tenant_email = 'Cynthia.Rachel@in.unisys.com,Srinivasa.Thota@in.unisys.com,Prashant.Vishwakarma@unisys.com,Unisysproject@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

# Schedule configuration - Daily at 7 AM UTC as per design doc
schedule_interval = '00 07 * * *'

# DAG identifiers
master_dag = f'unisys_fieldglass_time_export_master_{instance}_v1'
process_entries = f'unisys_fieldglass_time_export_process_entries_child_{instance}_v1'
export_generation = f'unisys_fieldglass_time_export_generation_child_{instance}_v1'
