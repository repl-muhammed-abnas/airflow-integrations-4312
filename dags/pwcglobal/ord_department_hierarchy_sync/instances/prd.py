# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.ord_department_hierarchy_sync.config import *

instance = 'prod'
region = 'eu-central-1'
environment = 'production'

company_key = 'pwc'

replicon_conn_id = 'pwcglobal-replicon-eu.automation'
sftp_conn_id = 'pwcglobal-MFT-PRD-replicon'

log_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/ORD/GCV/_logs'


tenant_email = 'gbl_replicon_support_team@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_log_email }}'


http_conn_id = f'pwc_ord_department_hierarchy_sync_{instance}_http_conn_id'
apikey = f'pwc_ord_department_hierarchy_sync_{instance}_apikey'
apikeysecret = f'pwc_ord_department_hierarchy_sync_{instance}_apikeysecret'
proxy_token_var = f'pwc_ord_department_hierarchy_sync_{instance}_proxy_token_var'
token_var = f'pwc_ord_department_hierarchy_sync_{instance}_token_var'


can_run_batch_task_var_name = f'pwc_ord_department_hierarchy_sync_{instance}_can_run_batch_task'
ord_mapper = f'pwc_ord_department_hierarchy_mapper_{instance}'
endpoint = '/GlobalCVService/GlobalCVService.svc/cv/ORD-RepliconSharingHierarchy'
