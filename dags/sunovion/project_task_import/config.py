
region = 'us-east-1'
environment = 'pre-production'
max_active_runs = 1
execution_timeout_days = 14
max_active_runs_process_each_code = 14

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

default_department_name = "Sumitomo Pharma America R&D"
master_dag_interval = 30
