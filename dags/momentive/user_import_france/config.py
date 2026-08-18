workday_report_name = 'ISU_Replicon/Worker_Changes_Data-_Replicon'

region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14

master_dag_active_runs = 1
process_each_user_trigger_parallel_count_master = 4
max_active_runs_process_each_user = 4

country = 'France'
time_zone = 'Europe/Paris'

# France eligibility filter applied to Workday records before syncing to Replicon.
# France filters on legal entity only (no exemption_status filter, unlike UAE).
eligible_legal_entity = 'MOMENTIVE PERFORMANCE MATERIALS FRANCE SARL'

# Replicon report holding the existing-user reference list (login/status/useruri/start-end dates).
# France resolves existing Replicon users from this report (Workato step 16 'userreferencereport'),
# instead of a per-user UserListService search.
report_name = '**userreferencereport**'
