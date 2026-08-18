# pylint: disable=wildcard-import unused-wildcard-import
from data_intellect_services.timeoff_sync_v1.config import *

instance = "prod"
environment = "production"
company_key = "DataIntellect"
replicon_conn_id = "dataintellect_replicon_admin"
http_conn_id = f"data_intellect_timeoff_sync_http_{instance}"

tenant_email = "hugh.mcShane@dataintellect.com,connor.metcalf@dataintellect.com,Evelina.Bakseviciute@dataintellect.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag = f'data_intellect_timeoff_sync_master_{instance}_v1'
timeoff_booking_child = f"data_intellect_timeoff_sync_timeoff_booking_child_{instance}_v1"
timeoff_delete_child = f"data_intellect_timeoff_sync_timeoff_delete_child_{instance}_v1"
