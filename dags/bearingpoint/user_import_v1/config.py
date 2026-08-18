region = 'eu-central-1'
environment = "pre-production"

process_payload_max_active_runs = 1
process_user_child_max_active_runs = 5
update_user_child_max_active_runs = 5
add_user_child_max_active_runs = 5
create_servicecenters_max_active_runs = 1
create_costcenters_max_active_runs = 1
create_departments_max_active_runs = 1
create_emptypes_max_active_runs = 1
create_locations_max_active_runs = 1
execution_timeout_days = 14

gather_logs_timeout_hours = 12
log_file_link_expiry = 7*24*60*60

time_zone = "Etc/UTC"

SUPERVISOR_PERMISSION = "Supervisor"
USER_PERMISSION = "Project Resource with Reports"
