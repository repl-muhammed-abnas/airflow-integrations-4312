"""
Trial instance configuration for Azenta Oracle PPM Time Export Integration (FI017)
Oracle UAT auth: HTTP Basic Auth (Authorization: Basic <base64>) per HANDOVER §2.1
"""
# pylint: disable=wildcard-import,unused-wildcard-import
from azenta.time_export.config import *

instance = "trial"
company_key = "AzentaUSInctrial01"
replicon_conn_id = "azentausinctrial01_replicon.repliconint"

# Oracle connection — UAT host: https://emmw-test.fa.us2.oraclecloud.com
# Auth: Basic Auth configured in the Airflow connection (no token fetch needed for UAT)
oracle_http_conn_id = "http_azentausinctrial01_oracle_uat"

# Email recipients
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_id = f"azenta_oracle_time_export_master_{instance}"
export_file_prefix = "AzentaUSInctrial01_TimeExport"

# Placeholder — confirm the real SFTP host/path with Azenta ops before go-live and create this
# Airflow Connection (analogous to the "Confirm ... before go-live" caveat on
# oracle_soap_action_validate_txn in config.py); no such connection exists yet.
report_sftp_conn_id = "sftp_useast2"
report_sftp_remote_dir = "/outbound/time_export_reports"
