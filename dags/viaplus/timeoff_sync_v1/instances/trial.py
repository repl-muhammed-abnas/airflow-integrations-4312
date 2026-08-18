# pylint: disable=wildcard-import unused-wildcard-import
from viaplus.timeoff_sync_v1.config import *

instance = "trial"
environment = "pre-production"
company_key = "ViaPlusLLCtrial01"
replicon_conn_id = "ViaPlusLLCtrial01_replicon_admin"
http_conn_id = f"viaplus_timeoff_sync_http_{instance}"
keka_login_conn_id = "keka_login_api"

keka_api_conn_id = "keka_api_trial"
KEKA_PAGE_SIZE = 1000

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# Keka API credentials stored in Airflow Variables
keka_client_id_var = f"viaplus_keka_client_id_{instance}"
keka_client_secret_var = f"viaplus_keka_client_secret_{instance}"
keka_api_key_var = f"viaplus_keka_api_key_{instance}"
keka_base_url_var = f"viaplus_keka_base_url_{instance}"

keka_conn_variables = f"viaplus_user_sync_client_details_{instance}"

# DAG names
master_dag = f'viaplus_timeoff_sync_master_{instance}_v1'
timeoff_booking_child = f"viaplus_timeoff_sync_timeoff_booking_child_{instance}_v1"
timeoff_delete_child = f"viaplus_timeoff_sync_timeoff_delete_child_{instance}_v1"

disabled = True