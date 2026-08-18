# Import common configurations
from capefoxcorporation.timesheet_sync.config import *
from capefoxcorporation.timesheet_sync.mappers.timeoff_project_by_employee_prefix import (
    timeoff_project_by_employee_prefix
)

# Instance-specific configuration
instance = 'sandbox'
company_key = 'capefoxcorporationsb'
replicon_conn_id = 'capefoxcorporationsb_replicon_integration'
deltek_costpoint_conn_id = 'capefoxcorporationsb_deltek_costpoint_32764'

# Instance-specific email configuration (production)
tenant_email = "csmith@infotekconsulting.net,culloa@capefoxss.com"
internal_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_failure_alert_email }}"

# Airflow Variable Names
can_run_batch_task_var_name = f'capefoxcorporation_deltek_costpoint_timesheet_sync_can_run_batch_task_{instance}'
group_by_project_var_name = f'capefoxcorporation_deltek_costpoint_timesheet_sync_group_by_project_{instance}'
pay_type_oef_var_name = f'capefoxcorporation_deltek_costpoint_timesheet_sync_pay_type_oef_name_{instance}'
lookup_log_timestamp_var = f'capefoxcorporation_deltek_costpoint_timesheet_sync_lookup_log_timestamp_{instance}'
log_generation_can_run_batch_task_var_name = f'capefoxcorporation_deltek_costpoint_timesheet_sync_log_generation_can_run_batch_task_{instance}'

# Timeoff sync configuration
is_sync_time_off_bookings = 'true'
timeoff_employee_prefix_mapping = timeoff_project_by_employee_prefix

# DAG ID Configuration
version = ""  # _v1, _v2, etc. will be appended to the end of DAG ids for new versions of the DAG. Keep empty for the initial version.
dag_id_prefix = f"{instance}{version}"

master_dag_id = f'capefoxcorporation_deltek_costpoint_timesheet_sync_master_{dag_id_prefix}'
log_generation_master_dag_id = f'capefoxcorporation_deltek_costpoint_timesheet_sync_log_generation_{dag_id_prefix}'
