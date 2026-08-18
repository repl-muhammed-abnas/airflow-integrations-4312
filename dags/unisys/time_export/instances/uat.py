"""
Development instance configuration for Unisys Fieldglass Time Export Integration
Contains only the configurations that are actually used in the integration

Based on design document: Replicon to Fieldglass Integration - Technical Specification V1.1
"""
# pylint: disable=wildcard-import unused-wildcard-import
from unisys.time_export.config import *

region = "us-east-1"

# Instance identification
instance = 'UAT'
company_key = 'UnisysUAT'  # As per design doc
replicon_conn_id = 'unisysuat_replicon_repliconint'
pgp_conn_id = 'pgp_unisys_time_export_oracle_uat'
secondary_pgp_conn_id = 'pgp_unisys_time_export_oracle_secondary_uat'

# SFTP configuration based on design doc
# Host: rsftp-useast.replicon.com
# Username: 710319
# Sandbox path: /Outbound/Fieldglass
sftp_conn_id = 'sftp_unisysuat_710319_UAT'
export_csv_filepath = '/Outbound/Fieldglass'
export_csv_to_secondary_filepath = '/Outbound/Fieldglass_Ops'

# Email configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

# Schedule configuration - Daily at 7 AM UTC as per design doc
schedule_interval = '00 07 * * *'

# DAG identifiers
master_dag = f'unisys_fieldglass_time_export_master_{instance}'
process_entries = f'unisys_fieldglass_time_export_process_entries_child_{instance}'
export_generation = f'unisys_fieldglass_time_export_generation_child_{instance}'
disabled = True