region = 'us-east-1'
environment = 'production'
instance = "production"
company_key = 'IntercontinentalExchange'
replicon_conn_id = 'IntercontinentalExchange_replicon_admin'
can_run_batch_task_var_name = f'IntercontinentalExchange_user_import_can_run_batch_task_{instance}'

pacific_timezone = 'US/Pacific'
schedule_interval_daily = '0 22 * * *'

user_report_name = 'User list - For Integration'
user_report_to_disable = "Enabled User list - For Disabling users"
user_managerhierarchy_report = "Managerhierarchy_Basereport"

sftp_conn_id = "sftp_IntercontinentalExchange_573892"
input_filepath = "/Production/User Demographic Data/Input"
log_filepath = "/Production/User Demographic Data/Log"
referance_filepath = "/Production/User Demographic Data/Reference"
archive_filepath = "/Production/User Demographic Data/Archive"

manage_hierarhy_input_filepath = "/Production/Manager Hierarchy/Input"
manage_hierarhy_referance_filepath = "/Production/Manager Hierarchy/Reference"
manage_hierarhy_archive_filepath = "/Production/Manager Hierarchy/Archive"
manage_hierarhy_log_filepath = "/Production/Manager Hierarchy/Log"

triggered_var = 'user_triggered_empids_ice_user_import'

threshold = 600

execution_timeout_days = 14
child_dag_max_active_runs = 2
master_dag_interval = 30

tenant_email = "ProdSupport-RepliconTimeManagement@theice.com"
# pylint: disable=line-too-long
tenant_email_for_user_import = "Suresh.paruchuri@ice.com,Naveen.nellimelli@ice.com,Vamseedhar.Kamireddy@ice.com,deji.anibaba@ice.com,Angela.Davidson@ice.com,Mohammed.Shaban@ice.com,ProdSupport-RepliconTimeManagement@theice.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
