# pylint: disable=wildcard-import unused-wildcard-import
from frontdoorinc.jd_export.config import *

instance = "prod"
environment = 'production'
company_key = 'frontdoorinc'

tenant_email = "Bryce.DeBruce@frontdoorhome.com"
bcc_tenant_email =  '{{ var.value.dagrun_internal_log_email }}'

replicon_conn_id = 'frontdoorinc_replicon_admin'

master_on_demand_dag_id = f"frontdoorinc_jd_export_master_on_demand_{instance}"
master_dag_id = f"frontdoorinc_jd_export_master_{instance}"
jd_export_child_dag_id = f"frontdoorinc_jd_export_child_{instance}"
jd_export_process_jelist_child = f"frontdoorinc_jd_export_process_jelist_{instance}"
