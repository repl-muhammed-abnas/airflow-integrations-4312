# pylint: disable=wildcard-import unused-wildcard-import
from altman.viexport.config import *

instance = 'production'
environment = 'production'

company_key = 'altman'

replicon_conn_id = 'altman_replicon_prateek'
sftp_conn_id = 'altman_sftp_AltmanSolon'

tenant_email = "Annette.McBride@altmansolon.com,Bshah@viglobal.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
