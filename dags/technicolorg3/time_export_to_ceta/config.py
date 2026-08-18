region = 'us-east-2'
environment = 'pre-production'

company_key = 'technicolortrial'
replicon_conn_id = 'replicon-technicolortrial'
instance = "trial"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_interval = '*/30 * * * *'
execution_timeout_days = 14

master_dag_max_active_run = 1

ceta_export_script_name = "CETA Export"
location_report_name = "User Location Assignment"

search_attributes = {
    "employee_type" : "Creative",
    "rssid" : "RSSID",
    "mill_mpc" : "Mill / MPC",
    "jira" : "Jira"
}
