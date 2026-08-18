# pylint: disable=wildcard-import unused-wildcard-import
from data_intellect_services.timeoff_sync_v1.config import *

instance = "trial"
environment = "pre-production"
company_key = "dataintellecttrial01"
replicon_conn_id = "dataintellecttrial01_replicon_admin"
http_conn_id = f"data_intellect_timeoff_sync_http_{instance}"
http_conn_id_search_employee = f"data_intellect_timeoff_sync_search_employee_http_{instance}"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

access_token = f"data_intellect_timeoff_sync_access_token_{instance}"

master_dag = f'data_intellect_timeoff_sync_master_{instance}_v1'
timeoff_booking_child = f"data_intellect_timeoff_sync_timeoff_booking_child_{instance}_v1"
timeoff_delete_child = f"data_intellect_timeoff_sync_timeoff_delete_child_{instance}_v1"

disabled=True
