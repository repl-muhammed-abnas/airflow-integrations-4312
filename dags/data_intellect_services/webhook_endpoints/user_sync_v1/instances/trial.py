# pylint: disable=wildcard-import unused-wildcard-import
from data_intellect_services.webhook_endpoints.user_sync_v1.config import *

instance = "trial"
environment = "pre-production"
company_key = "dataintellecttrial01"
replicon_conn_id = "dataintellecttrial01_replicon_admin"
data_intellect_hmac_shared_secret_user_create = f"dataintellecttrial01_user_sync_hmac_shared_secret_user_create_{instance}_v1"
data_intellect_hmac_shared_secret_user_update = f"dataintellecttrial01_user_sync_hmac_shared_secret_user_update_{instance}_v1"

http_conn_id = f"data_intellect_user_sync_http_{instance}"

user_sync_tenant_wide_log_name = f"data_intellect_user_sync_tenant_wide_log_{instance}_v1"
