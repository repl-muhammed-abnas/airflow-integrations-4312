region = 'eu-central-1'
instance = "trial"
environment = 'pre-production'

time_zone = "Etc/UTC"
log_file_link_expiry = 7*24*60*60
master_dag_active_runs = 1
child_dag_max_active_runs = 5
execution_timeout_days = 14
provider = 'bamboohr'
workflow = 'user_import'
schedule_interval = "0 */1 * * *"
user_permission_set = ["Project Resource"]
supervisor_permission_set = ["Project Resource with Reports", "Team Manager", "Supervisor", "Project Manager", "Resource Manager"]

STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
BAMBOOHR_LASTCHANGED_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
MDY_DATE_FORMAT = "%m/%d/%Y"
