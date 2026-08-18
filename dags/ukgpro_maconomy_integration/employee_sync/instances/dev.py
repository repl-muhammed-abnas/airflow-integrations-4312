"""Dev instance for UKG Pro → Maconomy Employee Sync.

Local-dev fallback so the DAGs parse without integration-platform-api committing
real per-customer instance files. In higher environments IPA writes one instance
file per customer into this folder via GitHub commits.
"""
# pylint: disable=invalid-name,unused-import,unused-wildcard-import,wildcard-import,import-error
from ukgpro_maconomy_integration.employee_sync.config import *  # noqa: F401,F403 — static per-recipe defaults

# --- Provenance (integration-platform-api identifiers) ---
CATALOG_ID = "ukgpro_maconomy_integration"
RECIPE_ID = "employee_sync"
CUSTOMER_ID = "cust001"

# --- Routing scope ---
region = 'us-east-1'
environment = 'pre-production'

# --- Customer identity ---
instance = 'cust001'
customer_slug = 'cust001'
customer_id = 'cust001'
deployment_slug = "dev"
customer_name = 'Cust001'
customer_email = None

# --- Integration identity ---
client_id = 'airflow_connector_proxy'
integration_id = '02327f89-f1bd-4d33-b0f8-02271c3b7e60'
integration_type = 'ukgpro_maconomy_integration__employee_sync'
company_key = "airflowsandboxuseast1"

# --- DAG IDs ---
create = "ukgpro_mn_employee_sync_create_cust001"
main = "ukgpro_mn_employee_sync_main_cust001"
router = "ukgpro_mn_employee_sync_router_cust001"
update = "ukgpro_mn_employee_sync_update_cust001"

# --- Airflow connections ---
ukgpro_conn_id = "mn_cust001_ukgpro_connid"
maconomy_conn_id = "mn_cust001_maconomy_connid"

# --- Scheduling / lifecycle ---
schedule_interval = None
disabled = False

# --- Notifications ---
notification_email = globals().get('tenant_email', None)

# --- Per-customer config ---
extras = {"execution_timeout_days": 3, "initial_sync_time": "2026-07-01T00:00:00.000Z", "max_active_runs": 1, "schedule_interval": "*/30 * * * *"}
execution_timeout_days = 3
initial_sync_time = '2026-07-01T00:00:00.000Z'
max_active_runs = 1
schedule_interval = "*/30 * * * *"
