region = 'us-east-1'
instance = 'pre-production'
environment = 'pre-production'

company_key = 'rosterfytrial01'
replicon_conn_id = 'rosterfytrial_replicon_admin'

execution_timeout_days = 14

max_active_runs_master = 1

presales_tasks_nonbillable = ['Pre-Sales']

sales_tasks_nonbillable = ['Pre-Sales', 'Onboarding', 'Expansion', 'Support']
sales_tasks_billable = ['Professional Services','Platform Enhancements']

service_tasks_nonbillable = ['Pre-Sales', 'Onboarding', 'Expansion', 'Support']
service_tasks_billable = ['Professional Services','Platform Enhancements']

renewals_tasks_nonbillable = ['Pre-Sales', 'Onboarding', 'Expansion', 'Support']
renewals_tasks_billable = ['Professional Services','Platform Enhancements']

renewals_master_dag_max_active_runs = 1
services_master_dag_max_active_runs = 1
sales_master_dag_max_active_runs = 1
update_project_master_dag_max_active_runs = 1

log_generation_dag_interval = "0 */3 * * *"
lookup_log_timestamp_hours:int = 3
