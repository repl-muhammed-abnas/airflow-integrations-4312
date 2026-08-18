region = 'us-east-1'
environment = 'pre-production'

company_key = 'KMHAVIntegrationIncafmig'

master_schedule_interval = 30
file_sensor_timeout = 10

max_active_runs_master = 1

alert_email = '{{ var.value.dagrun_internal_testing_email }}'
