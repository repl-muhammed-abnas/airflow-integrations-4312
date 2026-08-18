region = 'us-east-1'
environment = 'pre-production'
execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 4
# Keep these two in step with common_recipes_userimport/config.py::max_active_runs_child
process_each_user_trigger_parallel_count_master = 4
max_active_runs_process_each_user = 4

country = 'Belgium'
time_zone = "Europe/Brussels"

# Belgium eligibility filter applied to Workday records before syncing to Replicon
eligible_legal_entity = 'MOMENTIVE PERFORMANCE MATERIALS BENELUX BV'
eligible_exemption_status = '1'
