# pylint: disable=wildcard-import unused-wildcard-import
from frontdoorinc.jd_export.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'frontdoorincafmig'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

replicon_conn_id = 'frontdoorincafmig_replicon_admin'

master_on_demand_dag_id = f"frontdoorinc_jd_export_master_on_demand_{instance}"
master_dag_id = f"frontdoorinc_jd_export_master_{instance}"
jd_export_child_dag_id = f"frontdoorinc_jd_export_child_{instance}"
jd_export_process_jelist_child = f"frontdoorinc_jd_export_process_jelist_{instance}"
disabled = True
