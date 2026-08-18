instance = "trial"

region = "eu-central-1"
environment = "pre-production"
company_key = "macquarieproductiontrial01"

sftp_conn_id = "Airflow_migration_SFTP_eucentral"
replicon_conn_id = "macquarieproductiontrial01-replicon-tuser"
pgp_conn_id = "pgp_macquarieproductiontrial01_user-import"
recovery_reconciliation_reference_filename = "macquarie_recovery_reconciliation_reference_file.csv"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_active_runs = 1
child_dag_disableuser_max_active_runs = 16
add_departments_max_active_runs = 1

user_import_base_report_name = "***User Import Base report"

default_supervisor = "macquarieproduction_default_supervisor_id"
