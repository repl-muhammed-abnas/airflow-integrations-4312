# ── Environment & Instance Configuration ──────────────────
region = 'us-east-1'
environment = 'qa'
instance = "qa"

# ── Connection IDs ────────────────────────────────────────
company_key = f"airflowqasandbox{region.replace('-', '')}"
replicon_conn_id = "replicon_integrationtest_admin"
vantagepoint_conn_id = "vp_pdmdemo_replicon2_nik"

# ── Target Main DAG (User Sync) ───────────────────────────
target_user_sync_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_sync_main_{instance}'

# ── Schedule ──────────────────────────────────────────────
schedule_interval = '0 0 * * 0'  # Weekly: Sunday 00:00 UTC

# ── Email Notifications ───────────────────────────────────
# Airflow Variable holding recipient address(es) — set via Admin > Variables
email_recipients_variable = 'test_dag_email_recipients'

# ── VP Employee Test Data ─────────────────────────────────
vp_employee_company = '01'
vp_employee_org = 'Cohen Assoc Chicago Admin'

# ── Test DAG Employee Creation Constants ──────────────────
# Employee defaults
employee_status = 'A'  # Active
employee_type = 'E'    # Employee
employee_pay_type = 'H'  # Hourly
employee_ready_for_processing = 'Y'
employee_change_default_lc = 'Y'

# Employee naming
initial_sync_name_prefix = 'IntegTest'
webhook_name_prefix = 'WebhookTest'
webhook_updated_name_prefix = 'WebhookTestUpdated'

# Employee location & contact
employee_state = 'CT'
employee_country = 'US'
employee_locale = 'CT'
employee_hire_date = '2024-01-01'
employee_termination_date = '2024-06-01'
employee_email_domain = 'test.local'
