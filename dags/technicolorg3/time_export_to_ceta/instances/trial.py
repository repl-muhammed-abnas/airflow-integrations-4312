# pylint: disable=wildcard-import unused-wildcard-import
from technicolorg3.time_export_to_ceta.config import *

instance = "trial"


environment = 'pre-production'

company_key = 'technicolorg3afmig'
replicon_conn_id = 'replicon-technicolorg3afmig-admin'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

downstream_variable = f"technicolorg3_send_downstream_{instance}"
technicolor_timeexport_to_ceta_endpoint_mill = f"technicolorg3_timeexport_to_ceta_endpoint_{instance}"
technicolor_timeexport_to_ceta_endpoint_mpc = f"technicolorg3_timeexport_to_ceta_endpoint_{instance}"

disable=True

disabled=True
