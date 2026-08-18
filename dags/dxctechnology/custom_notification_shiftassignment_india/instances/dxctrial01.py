# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.custom_notification_shiftassignment_india.config import *
region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
instance = 'dxctrial01'
replicon_conn_id = 'replicon_dxctechnology_repliconintc1'
custom_notification_shiftassignment_child_dagid = 'dxctechnology_custom_notification_shiftassignment_child'
basereport_name = "***Custom_notification_shiftassignment_India"
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
execution_timeout_hours = 12

disable=True

disabled=True
