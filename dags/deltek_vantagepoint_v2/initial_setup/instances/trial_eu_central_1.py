from deltek_vantagepoint_v2.initial_setup.oef_mapper import get_oefs_with_required_name
from deltek_vantagepoint_v2.initial_setup.config import *
region = 'eu-central-1'
environment = 'pre-production'
instance = "trial"
company_key = f"airflowsandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowsandbox-replicon-admin'

execution_timeout_days = 14
max_active_runs = 10
can_run_batch_task_var_name = f'vp_replicon_initial_setup_can_run_batch_task_{instance}'
child_dag_max_active_runs = 10

root_department = 'Company'
replicon_export_file_format_name = 'Vantagepoint TimeData Export'

timesheet_field_oef_name_for_lc = 'Labor Codes'

workflow = 'initial_setup'
provider = 'vantagepoint'

hmac_secret = 'airflow_connector_ui_hmac_secret'
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_vantagepoint'

# DAG IDs
initial_setup_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_initial_setup_main_{instance}'
tag_oef_options_update_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_tag_oef_options_update_child_{instance}'
file_format_creation_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_file_format_creation_child_{instance}'
laborcategory_options_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_laborcategory_options_update_child_{instance}'
laborcode_options_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_laborcode_options_update_child_{instance}'
homecompany_group_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_homecompany_group_sync_child_{instance}'
user_sync_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_sync_main_{instance}'
webhook_creation_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_webhook_subscription_{instance}'
webhook_employee_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_webhook_employee_child_{instance}'
webhook_project_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_webhook_project_child_{instance}'
user_webhook_event_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_webhook_event_{instance}'
project_webhook_event_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_project_webhook_event_{instance}'

# Required OEFs format - "id": "preferred caption" or None to keep the default caption
oefs = get_oefs_with_required_name({
    "organization": None,
    "laborcategory": None,
    "laborcodelevel1": None,
    "laborcodelevel2": None,
    "laborcodelevel3": None,
    "laborcodelevel4": None,
    "laborcodelevel5": None,
    "laborcodecombined": timesheet_field_oef_name_for_lc,
    "yearsotherfirms": None,
    "prioryearsfirm": None,
    "allowlcupdate": None,
    "state": None,
    "country": None,
    "locale": None,
    "projectsupervisor": None,
    "projectprincipal": None,
    "workdistribution": None,
    "tkgroup": None
})

initial_setup_last_run_var = f'vp_replicon_initial_setup_last_run_{instance}'
user_webhook_url = f'https://webhooks-{environment}-{region}.replicon-integrations.com/webhooks/{user_webhook_event_dag_id}'
project_webhook_url = f'https://webhooks-{environment}-{region}.replicon-integrations.com/webhooks/{project_webhook_event_dag_id}'