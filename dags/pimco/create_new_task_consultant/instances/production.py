# pylint: disable=wildcard-import unused-wildcard-import
from pimco.create_new_task_consultant.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'PIMCO'
replicon_conn_id = 'pimco-replicon-production'

tenant_email = '''james.stone@pimco.com,david.edwards@pimco.com,alexandria.rausch@pimco.com,
                    scott.schwarmann@pimco.com,shekhar.gupta@pimco.com,
                        mayank.sharma@pimco.com'''
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
