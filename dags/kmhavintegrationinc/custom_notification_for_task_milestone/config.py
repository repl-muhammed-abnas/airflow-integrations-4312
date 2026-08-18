region = 'us-east-1'
environment = 'pre-production'

company_key = 'KMHAVIntegrationIncafmig'

schedule_interval = "0 10 * * *"
time_zone = "America/New_York"

max_active_runs_master = 1

milestone_details_report_name = "***Task - Milestone"
admin_details_report_name = "Adminemail"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
