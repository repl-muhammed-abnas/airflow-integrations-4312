from procore_ce_integration.job_structure_sync.utils.constants import WBSType

region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 7
max_active_runs = 1
max_active_runs_project_child = 5
max_active_runs_cop_child = 10

schedule_in_seconds = 60

aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

event_clean_interval_hours = 24
MAX_CHAR_LEN_DESCRIPTION = 60

procore_webhook_fmt = '%Y-%m-%dT%H:%M:%S.%fZ'

rfc_type = 'customer'

syncable_cop_statuses = ['approved']

allow_zero_amounts = False

sync_prime_contract_change_order = False # False if change order sync is enabled

sync_commitment_contract_change_order = False

subcontract_format = ''
is_paused_upon_creation = True
internal_email = ['procoreintegrationsupport@deltek.com']
