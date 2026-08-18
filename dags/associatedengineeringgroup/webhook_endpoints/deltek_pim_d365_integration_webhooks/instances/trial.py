# pylint: disable=wildcard-import,unused-wildcard-import
from associatedengineeringgroup.webhook_endpoints.deltek_pim_d365_integration_webhooks.config import *

instance = 'trial'
company_key = 'associatedengineeringgrouptrial'
environment = "pre-production"

# ---------------------------------------------------------------------------
# Webhook DAG IDs — format: associatedengineeringgroup_{type}_{what}_{master/child}_{instance}
# ---------------------------------------------------------------------------
lead_webhook_dag_id = f'associatedengineeringgroup_d365_pim_lead_webhook_master_{instance}'
internal_contact_webhook_dag_id = f'associatedengineeringgroup_d365_pim_internal_contact_webhook_master_{instance}'
opportunity_webhook_dag_id = f'associatedengineeringgroup_d365_pim_opportunity_webhook_master_{instance}'
enquiry_webhook_dag_id = f'associatedengineeringgroup_d365_pim_enquiry_webhook_master_{instance}'
project_webhook_dag_id = f'associatedengineeringgroup_d365_pim_project_webhook_master_{instance}'
external_org_webhook_dag_id = f'associatedengineeringgroup_d365_pim_external_org_webhook_master_{instance}'
external_contact_webhook_dag_id = f'associatedengineeringgroup_d365_pim_external_contact_webhook_master_{instance}'

# ---------------------------------------------------------------------------
# Child sync DAG IDs (triggered by webhooks)
# ---------------------------------------------------------------------------
lead_sync_child_dag_id = f'associatedengineeringgroup_d365_pim_lead_sync_child_{instance}'
internal_contact_sync_child_dag_id = f'associatedengineeringgroup_d365_pim_internal_contact_sync_child_{instance}'
opportunity_sync_child_dag_id = f'associatedengineeringgroup_d365_pim_opportunity_sync_child_{instance}'
enquiry_sync_child_dag_id = f'associatedengineeringgroup_d365_pim_enquiry_sync_child_{instance}'
project_sync_child_dag_id = f'associatedengineeringgroup_d365_pim_project_sync_child_{instance}'
external_org_sync_child_dag_id = f'associatedengineeringgroup_d365_pim_external_org_sync_child_{instance}'
external_contact_sync_child_dag_id = f'associatedengineeringgroup_d365_pim_external_contact_sync_child_{instance}'

# ---------------------------------------------------------------------------
# Webhook authentication (Airflow Variable name)
# ---------------------------------------------------------------------------
bearer_token_var = f'associatedengineeringgroup_d365_pim_webhook_bearer_token_{instance}'
