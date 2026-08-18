# pylint: disable=wildcard-import
from procore_ce_integration.initial_setup_sync.config import *
from procore_ce_integration.change_orders_sync.instances.wurster import webhook_dag_id as budget_revision_webhook_dag_id, budget_revision_events_key
from procore_ce_integration.subcontract_change_order_sync.instances.wurster import webhook_processing_dag_id as change_order_webhook_dag_id, change_order_events_key
from procore_ce_integration.job_structure_sync.instances.wurster import webhook_processing_dag_id as job_webhook_dag_id, job_structure_events_key

instance = 'wurster'
environment = 'production'
region = 'us-east-1'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
initial_setup_main_dag_id = f'procore_ce_initial_setup_sync_main_{instance}'
webhook_subscribing_child_dag_id = f'procore_ce_webhook_subscribing_child_{instance}'
attachment_child_dag_id = f'procore_ce_attachment_upload_child_dag_{instance}'

# Email configurations
tenant_email = ['sdean@wursterconstruction.com']
internal_email = ['procoreintegrationsupport@deltek.com']

bearer_token_var = f'procore_ce_webhook_token_{instance}'

# S3 event storage
webhooks_s3_directory = f'Procore_CE/{environment}/{instance}/webhooks/'

# Webhook configurations
webhook_subscriptions = [
    {
        'namespace': f'budget-revision-sync-webhook-{environment}-{instance}',
        'destination_url': f'https://webhooks-{environment}-{region}.replicon-integrations.com/webhooks/{budget_revision_webhook_dag_id}',
        'api_version': 'v2',
        'triggers': list({
            'resource_name': 'Change Order Packages',
            'event_type': event_type
        } for event_type in ['create', 'update']) +
        list({
            'resource_name': 'Change Events',
            'event_type': event_type
        } for event_type in ['create', 'update']) +
        list({
            'resource_name': 'Budget Changes',
            'event_type': event_type
        } for event_type in ['create', 'update']),
        's3_files': [budget_revision_events_key]
    },
    {
        'namespace': f'changeorder-sync-webhook-{environment}-{instance}',
        'destination_url': f'https://webhooks-{environment}-{region}.replicon-integrations.com/webhooks/{change_order_webhook_dag_id}',
        'api_version': 'v2',
        'triggers': list({
            'resource_name': 'Change Order Packages',
            'event_type': event_type
        } for event_type in ['create', 'update']),
        's3_files': [change_order_events_key]
    },
    {
        'namespace': f'job-sync-webhook-{environment}-{instance}',
        'destination_url': f'https://webhooks-{environment}-{region}.replicon-integrations.com/webhooks/{job_webhook_dag_id}',
        'api_version': 'v2',
        'triggers': (
            list({
                'resource_name': 'Projects',
                'event_type': event_type
            } for event_type in ['create', 'update']) +
            list({
                'resource_name': 'Cost Codes',
                'event_type': event_type
            } for event_type in ['create', 'update']) +
            list({
                'resource_name': 'Budget Line Items',
                'event_type': event_type
            } for event_type in ['create', 'update']) +
            list({
                'resource_name': 'Prime Contracts',
                'event_type': event_type
            } for event_type in ['create', 'update'])
        ),
        's3_files': [job_structure_events_key]
    }
]
