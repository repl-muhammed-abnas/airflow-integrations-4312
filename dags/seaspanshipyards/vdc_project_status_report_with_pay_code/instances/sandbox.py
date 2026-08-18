# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.vdc_project_status_report_with_pay_code.config import *
environment = "pre-production"
instance = "sandbox"
replicon_conn_id = 'seaspanshipyardssb_replicon_rnadmin'
company_key = "Seaspanshipyardssb"
webhook_shared_secret = f"seaspanshiyards_vdc_project_status_report_with_pay_code_webhook_secret_{instance}"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'