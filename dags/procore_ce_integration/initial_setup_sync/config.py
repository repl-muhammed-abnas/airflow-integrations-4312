from procore_ce_integration.change_orders_sync.config import s3_collection as budget_revision_collection
from procore_ce_integration.purchase_order_sync.config import s3_collection as purchase_order_collection
from procore_ce_integration.job_structure_sync.config import s3_collection as job_structure_collection
from procore_ce_integration.ap_invoice_sync_v2.config import s3_collection as ap_invoice_collection
from procore_ce_integration.vendors_sync.config import s3_collection as vendor_collection

region = 'us-east-1'
environment = 'pre-production'

aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

# S3 collections created once per tenant during initial setup. Append future
# collections here; each is {integration, tables}.
collections_to_create = [
    budget_revision_collection,
    purchase_order_collection,
    job_structure_collection,
    ap_invoice_collection,
    vendor_collection
]

execution_timeout_days = 1
max_active_runs = 1
max_active_runs_attachment_child = 10

PROCORE_SELF_APP_ID_PREFIX_LEN = 8
PROCORE_SELF_APP_ID_PREFIX_HASHES = (
    '30b9af83597422216b447d40b5822cbfc6c94b0999ef4684f3a250cb5275dd62',  # prod
    '7c50501bb35f2ef4641b787560dae3d3e578c7504e3e3d4391cee666973d0e54',  # sandbox
    '61abe8c2788914a2c35220877900cf7e1948051710bbfc7e1c145bb4e0f88aa7',  # sandbox
)
is_paused_upon_creation = True
internal_email = ['procoreintegrationsupport@deltek.com']
