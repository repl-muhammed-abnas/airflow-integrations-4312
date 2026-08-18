# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.ord_department_hierarchy_sync.config import *

instance = 'dev'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'pwcdev'

replicon_conn_id = 'pwcdev-replicon-eu.automation'
sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'

log_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Inbound/ORD/GCV/_logs'


tenant_email = 'PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'


http_conn_id = f'pwc_ord_department_hierarchy_sync_{instance}_http_conn_id'
apikey = f'pwc_ord_department_hierarchy_sync_{instance}_apikey'
apikeysecret = f'pwc_ord_department_hierarchy_sync_{instance}_apikeysecret'
proxy_token_var = f'pwc_ord_department_hierarchy_sync_{instance}_proxy_token_var'
token_var = f'pwc_ord_department_hierarchy_sync_{instance}_token_var'


can_run_batch_task_var_name = f'pwc_ord_department_hierarchy_sync_{instance}_can_run_batch_task'
ord_mapper = f'pwc_ord_department_hierarchy_mapper_{instance}'


schedule_timezone_Aukland = "Europe/Paris"
endpoint = '/GlobalCVService/GlobalCVService.svc/cv/Integration_ORD-RepliconSharingHierarchy'
