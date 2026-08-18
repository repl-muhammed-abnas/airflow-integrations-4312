
region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
max_active_runs = 1
child_dag_max_active_runs = 5

# CE import statuses to skip
SKIP_STATUSES = ['accepted', 'downloaded', 'received']

default_item_description = 'Procore Invoiced Amount'

# Default WBS type - prevents import errors when job_structure_sync (Procore→CE) is not deployed.
# This should be overriden in instance file if job_structure_sync (Procore→CE) is deployed.
default_wbs_type = 'Job/Phase/Cat'
is_paused_upon_creation = True

# ComputerEase AR Invoice field validation configuration (based on XSD schema)
CE_AR_FIELD_VALIDATIONS = {
    'client_code': {'display_name': 'Customer Code', 'char_limit': 8, 'field_type': 'invoice', 'truncate': False},
    'invoice_number': {'display_name': 'Invoice Number', 'char_limit': 8, 'field_type': 'invoice', 'truncate': False},
    'job_code': {'display_name': 'Job Code', 'char_limit': 10, 'field_type': 'invoice', 'truncate': False},
    'description': {'display_name': 'Description', 'char_limit': 30, 'field_type': 'invoice', 'truncate': True},

    # Distribution fields
    'phase_code': {'display_name': 'Phase Code', 'char_limit': 4, 'field_type': 'distribution', 'truncate': False},
    'category_code': {'display_name': 'Category Code', 'char_limit': 6, 'field_type': 'distribution', 'truncate': False},
}
internal_email = ['procoreintegrationsupport@deltek.com']
