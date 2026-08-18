from associatedengineeringgroup.deltek_pim_d365_integration.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'associatedengineeringgrouptrial'

# Connection IDs
d365_auth_conn_id = 'associatedengineeringgroup_d365_auth_trial'
d365_conn_id = 'associatedengineeringgroup_d365_trial'
pim_conn_id = 'associatedengineeringgroup_pim_apiuserqa'


# Notification
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# DAG IDs — format: associatedengineeringgroup_{type}_{what}_{master/child}_{instance}
token_refresh_dag_id = f'associatedengineeringgroup_d365_pim_token_refresh_master_{instance}'
error_log_dag_id = f'associatedengineeringgroup_d365_pim_error_log_child_{instance}'
lead_dag_id = f'associatedengineeringgroup_d365_pim_lead_sync_child_{instance}'
project_dag_id = f'associatedengineeringgroup_d365_pim_project_sync_child_{instance}'
internal_contact_dag_id = f'associatedengineeringgroup_d365_pim_internal_contact_sync_child_{instance}'
opportunity_dag_id = f'associatedengineeringgroup_d365_pim_opportunity_sync_child_{instance}'
external_org_dag_id = f'associatedengineeringgroup_d365_pim_external_org_sync_child_{instance}'
external_contact_dag_id = f'associatedengineeringgroup_d365_pim_external_contact_sync_child_{instance}'
process_udfs_dag_id = f'associatedengineeringgroup_d365_pim_process_udfs_child_{instance}'
enquiry_dag_id = f'associatedengineeringgroup_d365_pim_enquiry_sync_child_{instance}'
