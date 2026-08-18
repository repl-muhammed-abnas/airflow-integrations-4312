region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
child_dag_max_active_runs = 5
job_child_dag_max_active_runs = 5
phase_child_dag_max_active_runs = 5
category_child_dag_max_active_runs = 5
max_active_runs = 1

# Job sync specific settings
country_code = 'US'
enable_copy_of_standard_cost_codes = False

# Project template configuration
project_template_udf_field_name = 'Procore Project Template'
default_project_template_name = 'Standard Project Template'  # Fallback template name

# Common settings
ce_time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
initial_sync_time = '1970-01-01T00:00:00.000Z'

job_sync_interval_minutes = 10

cost_code_segment_type = 'cost_code'
cost_code_segment_name = 'Cost Code'

#Acting as a feature flag for now and gets set for the instances we want to roll out to.
#Once tested, will be removed from config and defined in each instance file.
job_child_dag_v2_id = None
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
