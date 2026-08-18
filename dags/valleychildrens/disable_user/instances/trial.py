# pylint: disable=wildcard-import unused-wildcard-import
from valleychildrens.disable_user.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'ValleyChildrensafmig'
replicon_conn_id = 'ValleyChildrensafmig_replicon_admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
