# pylint: disable=wildcard-import
from procore_ce_integration.initial_setup_sync.config import *
from procore_ce_integration.ap_invoice_sync.instances.sushmamathi1 import ap_invoice_main_dag_id as ap_invoice_dag_id, ap_invoice_events_key
from procore_ce_integration.productivity_unit_sync.instances.sushmamathi1 import webhook_dag_id as productivity_unit_dag_id, productivity_unit_events_key
from procore_ce_integration.vendors_sync.instances.sushmamathi1 import vendor_webhook_dag_id

instance = 'sushmamathi1'
environment = 'pre-production'
region = 'us-east-1'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

initial_setup_main_dag_id = f'procore_ce_initial_setup_sync_main_{instance}'
webhook_subscribing_child_dag_id = f'procore_ce_webhook_subscribing_child_{instance}'
attachment_child_dag_id = f'procore_ce_attachment_upload_child_dag_{instance}'

tenant_email = ['sushmamathi@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']

bearer_token_var = f'procore_ce_webhook_token_{instance}'

# S3 event storage
webhooks_s3_directory = f'Procore_CE/{environment}/{instance}/webhooks/'

webhook_subscriptions = [
    {
        'namespace': f'apinvoice-sync-webhook-{environment}-{instance}',
        'destination_url': f'https://webhooks-{environment}-{region}.replicon-integrations.com/webhooks/{ap_invoice_dag_id}',
        'api_version': 'v2',
        'triggers': list({'resource_name': 'Draw Requests', 'event_type': event_type} for event_type in ['create', 'update']),
        's3_files': [ap_invoice_events_key]
    },
    {
        'namespace': f'productivity-unit-sync-webhook-{environment}-{instance}',
        'destination_url': f'https://webhooks-{environment}-{region}.replicon-integrations.com/webhooks/{productivity_unit_dag_id}',
        'api_version': 'v2',
        'triggers': [{'resource_name': 'Productivity Logs', 'event_type': 'create'}],
        's3_files': [productivity_unit_events_key]
    },
    {
        'namespace': f'vendor-sync-webhook-{environment}-{instance}',
        'destination_url': f'https://webhooks-{environment}-{region}.replicon-integrations.com/webhooks/{vendor_webhook_dag_id}',
        'api_version': 'v2',
        'triggers': list({
            'resource_name': 'Company Vendors',
            'event_type': event_type
        } for event_type in ['create', 'update'])
    }
]
