"""
Configuration settings for the T-Systems Cost Center Hierarchy Import integration.
"""

region = "eu-central-1"
environment = "pre-production"

# Child DAG specific configurations
child_dag_max_active_runs = 3
max_active_runs = 1
intermediate_child_dag_max_active_runs = 1

# CSV specifications
csv_delimiter = ';'
csv_columns = ['Name', 'Code', 'Description', 'Status', 'Cost Center Manager']
required_columns = ['Name', 'Code', 'Description', 'Status']

child_dag_timeout_hours = 12
file_sensor_timeout = 10
timezone = 'Etc/UTC'
