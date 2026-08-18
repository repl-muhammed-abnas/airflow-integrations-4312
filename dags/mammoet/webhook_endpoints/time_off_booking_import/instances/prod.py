# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.webhook_endpoints.time_off_booking_import.config import *

instance = "prod"

environment = "production"

company_key = "mammoet"
replicon_conn_id = "mammoet_replicon_admin"

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

webhook_master_dagid = f"mammoet_timeoff_booking_import_webhook_master_{instance}"
process_timeoff_import_payload_dagid = f"mammoet_timeoff_booking_import_process_payload_child_{instance}_v2"

mammoet_timeoff_booking_import_bearer_token_var = f"mammoet_timeoff_booking_import_bearer_token_variable_{instance}"
