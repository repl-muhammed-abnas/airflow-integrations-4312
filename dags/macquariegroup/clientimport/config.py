instance = 'trial'

region = 'eu-central-1'
environment = 'pre-production'
company_key = 'macquarieproductiontrial01'

sftp_conn_id = 'Airflow_migration_SFTP_eucentral'
replicon_conn_id = 'macquarieproductiontrial01-replicon-ltran17'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

timezone = 'Australia/Sydney'
schedule_interval = "0 9 * * *"

input_filepath = '/macquarie/clientimport/input'
log_filepath = '/macquarie/clientimport/logs'
reference_filepath = '/macquarie/clientimport/reference'
archive_filepath = '/macquarie/clientimport/archive'

client_csv_file = 'Client.csv'
bu_csv_file = 'BU.csv'
locations_csv_file = 'Locations.csv'
reference_client_csv_file = 'Reference_Client.csv'

client_import_log_file = 'ClientImportlogs.csv'


# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'
execution_timeout_days = 14
max_active_runs = 1
child_dag_active_runs = 1

client_report_for_integration = 'Client Report - For Integration'

delta_threshold = 10000
batch_size = 2000
