region = 'us-east-1'
environment = 'pre-production'

instance = "horizonmediagen3afmig"

company_key = 'horizonmediagen3afmig'
replicon_conn_id = 'horizonmediagen3afmig_replicon_admin'

can_run_batch_task_var_name = f'horizonmediagen3afmig_user_import_can_run_batch_task_{instance}'

base_report_name = 'BaseReport_SupervisorORG_Group_Assignment'

execution_timeout_days = 14
child_dag_max_active_runs = 20

# "input": {
#         "time_unit": "weeks",
#         "trigger_every": "1",
#         "days_of_week": "1,2,3,4,5",
#         "trigger_at": "08:30:00",
#         "timezone": "America/New_York"
#     },
#     "param": {
#         "companyKey": "HorizonMediaTrial01",
#         "emailid": "workday@horizonmedia.com",
#         "logpath": "/User Sync/Log Files"
#     },
schedule_time_zone = 'EST'
schedule_interval = '30 8 * * 1-5'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
