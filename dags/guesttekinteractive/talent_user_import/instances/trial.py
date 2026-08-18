"""
Trial Instance Configuration - GuestTek Talent User Import Integration

Instance-specific configuration for the trial environment of the GuestTek Talent
User Import integration. This module inherits base configuration from config.py and
overrides instance-specific settings.

Configuration includes:
    - Instance identification and naming
    - Replicon connection settings
    - Talent API connection settings
    - Email notification settings
    - DAG identifiers for all child DAGs
    - Feature flags and control variables

Key Settings:
    - instance: 'trial'
    - company_key: 'guesttekinteractivetrial01'
    - Talent API Base URL: https://pre-soadevca.tm.deltek.com
"""
# pylint: disable=wildcard-import unused-wildcard-import
from guesttekinteractive.talent_user_import.config import *

region = 'us-east-1'
environment = 'pre-production'

# Instance identification
instance = 'trial'
company_key = 'guesttekinteractivetrial01'
replicon_conn_id = 'guesttekinteractivetrial01_integration_replicon'

# Talent API configuration
talent_api_base_url = 'https://pre-soadevca.tm.deltek.com'
talent_conn_id = 'guesttek_talent_api_trial'

# Email configuration
tenant_email = 'Replicongtk@guest-tek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

# DAG identifiers
master_dag = f'guesttek_talent_user_import_master_{instance}'
process_each_user = f'guesttek_talent_user_import_process_users_child_{instance}'
process_new_users = f'guesttek_talent_user_import_process_new_users_child_{instance}'
process_update_users = f'guesttek_talent_user_import_process_update_users_child_{instance}'
processs_supervisor = f'guesttek_talent_user_import_process_supervisor_child_{instance}'
process_log_generation = f'guesttek_talent_user_import_process_log_generation_child_{instance}'
process_groups_dag_id = f'guesttek_talent_user_import_process_groups_child_{instance}'
process_new_usertypes = f'guesttek_talent_user_import_process_usertypes_child_{instance}'
process_employeetype = f'guesttek_talent_user_import_process_employeetype_child_{instance}'

# Role DAG identifiers
process_roles_dag_id = f'guesttek_talent_user_import_process_roles_child_{instance}'
process_each_role_dag_id = f'guesttek_talent_user_import_process_role_child_{instance}'

# Service Center DAG identifiers
process_service_centers_dag_id = f'guesttek_talent_user_import_process_servicecenters_child_{instance}'
process_each_service_center_dag_id = f'guesttek_talent_user_import_process_servicecenter_child_{instance}'

# Feature flags
can_run_batch_task = f'guesttek_talent_user_import_can_run_batch_task_{instance}'

# Event-log polling state
last_processed_time_var = f'guesttek_talent_user_import_last_processed_time_{instance}'
