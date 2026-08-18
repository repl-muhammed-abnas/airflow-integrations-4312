from fujifilmdbtl.rehire_logic_after_1_year.config import *

environment = 'pre-production'

instance = 'trial'

company_key='FUJIFILMDBTLafmig'

replicon_conn_id = "fujifilmdbtlafmig_replicon_admin"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag=f'fujiflimdbtl_rehire_logic_master_{instance}'
child_dag=f'fujiflimdbtl_rehire_logic_update_employment_date_range_child_{instance}'
subchild_dag=f'fujifilmdbtl_rehire_logic_update_timeofftype_for_user_subchild_{instance}'

