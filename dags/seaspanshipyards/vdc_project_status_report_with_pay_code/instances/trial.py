# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.vdc_project_status_report_with_pay_code.config import *
environment = "pre-production"
instance = "trial"
replicon_conn_id = 'seaspanshipyardsafmig-replicon-admin'
company_key = "SeaspanShipyardsafmig"
webhook_shared_secret = f"seaspanshiyards_vdc_project_status_report_with_pay_code_webhook_secret_{instance}"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
disabled=True
