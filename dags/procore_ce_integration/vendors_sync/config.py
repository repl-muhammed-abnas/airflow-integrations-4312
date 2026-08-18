region = 'us-east-1'
environment = 'pre-production'
execution_timeout_days = 7
max_active_runs = 1

# Procore webhook resource/event the vendor webhook DAG handles.
resource_company_vendors = 'Company Vendors'
supported_event_types = ('create', 'update')

# Mode-switch Airflow Variable (per instance): empty/unset -> webhook mode;
# a date (YYYY-MM-DD) -> one-time bulk reconciliation of all vendors updated since
# that date. The DAG assembles the full per-instance name as
# f'{vendor_bulk_sync_trigger_date_var_prefix}_{instance}' because `instance` is only
# defined in the instance modules, not here.
vendor_bulk_sync_trigger_date_var_prefix = 'procore_ce_vendor_bulk_sync_trigger_date'

vennum_max_length = 8

# ISO-8601 UTC timestamp format for the bulk filters[updated_at] range sent to Procore.
procore_datetime_format = '%Y-%m-%dT%H:%M:%SZ'

# ComputerEase vendor status mappings
vendor_status_active = "1"
vendor_status_inactive = "2"

cus_identifier = 'CUS - '

# True: set origin_id in Procore only after CE accepts the import (vendor mark ERP sync DAG).
defer_origin_id_until_accepted = False
origin_id_update_schedule_seconds = 300
is_paused_upon_creation = True

# Worklist of vendor links awaiting CE acceptance; webhook/mark-erp DAGs read/write rows.
# Keyed on vennum (abbreviated_name); a bulk import shares one import_uuid across many rows.
origin_id_update_table = {
    'name': 'pending_origin_id_update',
    'columns': ['vennum', 'procore_vendor_id', 'origin_id', 'import_uuid', 'queued_at'],
    'unique_columns': ['vennum'],
    'source': []
}

s3_collection = {
    'integration': 'procore_ce_vendor_sync',
    'tables': [origin_id_update_table]
}

# Field validation rules for vendor data
field_validations = [
    {'field': 'address1', 'display_name': 'Address1', 'max_length': 30, 'truncate': True},
    {'field': 'address2', 'display_name': 'Address2', 'max_length': 30, 'truncate': True},
    {'field': 'city', 'display_name': 'City', 'max_length': 20, 'truncate': True},
    {'field': 'zip', 'display_name': 'Zip', 'max_length': 10, 'truncate': True},
    {'field': 'phone', 'display_name': 'Phone', 'max_length': 10, 'truncate': True},
    {'field': 'email', 'display_name': 'Email', 'max_length': 60, 'truncate': True},
    {'field': 'fax', 'display_name': 'Fax', 'max_length': 10, 'truncate': True},
    {'field': 'web', 'display_name': 'Web', 'max_length': 40, 'truncate': True}
]
internal_email = ['procoreintegrationsupport@deltek.com']
