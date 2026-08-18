from mci.user_import.mappers import mci_usersync_mapper
region = 'us-east-1'
environment = 'pre-production'
company_key = 'MCIafmig'
max_active_runs=1
max_active_runs_child=1

execution_timeout_days=14
master_dag_interval = 30
user_detail_report = 'User Detail Report'
mapper=mci_usersync_mapper.mapper
