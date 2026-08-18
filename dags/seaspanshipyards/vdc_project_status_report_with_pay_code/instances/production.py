# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.vdc_project_status_report_with_pay_code.config import *
environment = "production"
instance = "production"
replicon_conn_id = 'seaspanshipyards_replicon_admin'
company_key = "SeaspanShipyards"
webhook_shared_secret = f"seaspanshiyards_vdc_project_status_report_with_pay_code_webhook_secret_{instance}"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'