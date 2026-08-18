from procore_ce_integration.job_structure_sync.utils.constants import WBSType
region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 1
max_active_runs = 1
max_active_runs_child = 5

schedule_seconds = 60

# S3 settings
aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

# Webhook settings
procore_webhook_fmt = '%Y-%m-%dT%H:%M:%S.%fZ'

# Event retention settings
event_retention_days = 7
event_clean_interval_hours = 24

# Default WBS type used when project is first created in Procore (job not found in ComputerEase)
default_wbs_type = WBSType.JOB_PHASE_CAT

# Address splitting settings
address_max_length = 30
address_max_lines = 3

# Integration settings
integration_type = 'generic'

# Full sync settings
sync_all_cost_codes = False
budget_view_name = 'Procore Standard Budget'

# Origin ID update settings
# True: write origin_id to Procore only after CE accepts the import (mark ERP
# sync DAG). False: update eagerly on send (that DAG stays inert). Default False;
# enabled per-instance (qa1/qa2/qa3) for testing before flipping the default.
defer_origin_id_until_accepted = False
origin_id_update_schedule_seconds = 300
is_paused_upon_creation = True

# Worklist of links awaiting CE acceptance; main/update DAGs read/write rows.
origin_id_update_table = {
    'name': 'pending_origin_id_update',
    'columns': ['project_number', 'procore_project_id',
                'origin_id', 'import_uuid', 'queued_at'],
    'unique_columns': ['project_number'],
    'source': []
}

s3_collection = {
    'integration': 'procore_ce_job_structure_sync',
    'tables': [origin_id_update_table]
}

# Prime Contract settings
revenue_cost_type = 'REVENUE'
# When True, sends contractamount=0 for cost codes with no prime contract line item, clearing removed amounts in CE
support_contract_line_item_removal = True

# ComputerEase XML field character limits (based on XSD schema)
FIELD_CHAR_LIMITS = {   # Job level fields - (limit, bypass_flag)
    'project_number': (10, False),
    'name': (30, True),
    'address1': (30, True),
    'address2': (30, True),
    'address3': (30, True),
    'city': (20, True),
    'state_code': (2, True),
    'zip': (10, True),
}

PHASE_LIMITS = {   # Phase fields - (limit, bypass_flag)
    'code': (4, False),
    'name': (30, True),
}

CATEGORY_LIMITS = {  # Category fields - (limit, bypass_flag)
    'code': (6, False),
    'name': (30, True),
}
internal_email = ['procoreintegrationsupport@deltek.com']
