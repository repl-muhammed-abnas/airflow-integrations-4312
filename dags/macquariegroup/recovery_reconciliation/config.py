instance = "trial"

region = "eu-central-1"
environment = "pre-production"
company_key = "macquarieproductiontrial01"

sftp_conn_id = "Airflow_migration_SFTP_eucentral"
replicon_conn_id = "macquarieproductiontrial01-replicon-tuser"

user_base_report = "***Reconciliation User Base Report"
recovery_reconciliation_reference_filename = "macquarie_recovery_reconciliation_reference_file.csv"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

execution_timeout_days = 14
