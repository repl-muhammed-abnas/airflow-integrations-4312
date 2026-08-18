workday_report_name = 'ISU_Replicon/Worker_Changes_Data-_Replicon'

region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14

master_dag_active_runs = 1
# Fan-out width for per-user processing. Keep these two in step with
# common_recipes_userimport/config.py::max_active_runs_child - that value is the shared
# ceiling for the add/update/disable children across every country using those DAGs, so
# raising the width here without raising it there just queues the children (and raising both
# increases sustained Replicon call volume for the tenant).
process_each_user_trigger_parallel_count_master = 4
max_active_runs_process_each_user = 4

country = 'UAE'
time_zone = 'Asia/Dubai'
