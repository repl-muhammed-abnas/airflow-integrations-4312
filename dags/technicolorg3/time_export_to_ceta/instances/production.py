# pylint: disable=wildcard-import unused-wildcard-import
from technicolorg3.time_export_to_ceta.config import *

instance = "production"

environment = 'production'

company_key = 'TechnicolorG3'
replicon_conn_id = 'replicon-technicolorG3-admin'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

downstream_variable = f"technicolorg3_send_downstream_{instance}"
technicolor_timeexport_to_ceta_endpoint_mill = f"technicolorg3_timeexport_to_ceta_endpoint_mill_{instance}"
technicolor_timeexport_to_ceta_endpoint_mpc = f"technicolorg3_timeexport_to_ceta_endpoint_mpc_{instance}"
disabled = True
