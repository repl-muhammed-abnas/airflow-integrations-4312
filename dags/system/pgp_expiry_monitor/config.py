"""
Configuration for PGP Key Expiry Monitor DAG.

This DAG monitors PGP key expiry dates from Airflow connections and sends
email alerts when keys are expiring within 15-30 days or already expired.
"""
from datetime import timedelta

# Environment Settings
region = 'all'
environment = ['pre-production', 'production']

# DAG Settings
dag_id = 'system_pgp_key_expiry_monitor'
schedule = '0 8 1,15 * *'  # 1st and 15th of every month at 8 AM
max_active_runs = 1

# Expiry Threshold (alert for keys expiring between min and max days)
# Running on 1st and 15th with 15-30 day window ensures each key is alerted once
expiry_warning_days_min = 15
expiry_warning_days_max = 30

# Airflow Variable Names
can_run_batch_task_var_name = 'system_pgp_key_expiry_monitor_can_run_batch_task'
excluded_pgp_conn_ids_var_name = 'system_pgp_key_expiry_monitor_excluded_pgp_conn_ids'

# Email Settings
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# Default Args for Tasks
default_args = {
    'owner': 'system',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
