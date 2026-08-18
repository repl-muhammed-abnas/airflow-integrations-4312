from michaelkorstna.uk_user_import.mappers.michael_kors_gmbh_user_sync_master_mapper_uk import michael_kors_gmbh_user_sync_master_mapper_uk
region = 'eu-central-1'
environment = 'pre-production'
company_key = 'Michaelkorstnaafmig'
replicon_conn_id = 'michaelkorstnaafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'

max_active_runs=1
execution_timeout_days = 14
userlist_to_disable_report = '***User List to disable***'
user_timeoffbooking_report = '***User Timeoff Booking List***'
jobtype = 'UK user sync'

master_mapper = michael_kors_gmbh_user_sync_master_mapper_uk
