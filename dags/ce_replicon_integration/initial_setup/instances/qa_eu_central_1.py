from ce_replicon_integration.initial_setup.oef_mapper import get_oefs_with_required_name
from ce_replicon_integration.initial_setup.config import *
from ce_replicon_integration.main_dag.instances.qa_eu_central_1 import initial_setup_last_run_var 


region = 'eu-central-1'
environment = 'qa'
instance = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowqasandbox-replicon-admin'

hmac_secret = 'airflow_connector_ui_hmac_secret'

employee_last_sync_time_var = f'ce_replicon_employee_sync_last_sync_time_{instance}'
initial_setup_last_run_var = f'ce_replicon_initial_setup_last_run_{instance}'
execution_timeout_days = 14
max_active_runs = 5
child_dag_max_active_runs = 10

replicon_export_file_format_name = 'ComputerEase Time Export'

initial_setup_dag_id = f"standard_computerease_{region.replace('-', '_')}_initial_setup_main_{instance}"
union_group_child_dag_id = f"standard_computerease_{region.replace('-', '_')}_initial_setup_union_group_child_{instance}"
worker_class_child_dag_id = f"standard_computerease_{region.replace('-', '_')}_initial_setup_worker_class_child_{instance}"
file_format_creation_child_dag_id = f"standard_computerease_{region.replace('-', '_')}_initial_setup_file_format_creation_child_{instance}"
tag_oef_options_update_child_dag_id = f"standard_computerease_{region.replace('-', '_')}_initial_setup_tag_oef_options_update_child_{instance}"

workflow = 'initial_setup'
provider = 'computerease'

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_computerease'

# Required OEFs format - "id": "preferred caption" or None to keep the default caption
oefs = get_oefs_with_required_name({
    "useridentify": None,
    "companyuuid": None,
    "paytype": None,
    "unionworkerclass": None,
    "payrolldate": None,
    "wbstype": None,
    "amount": None,
    "workpayment": None
})
