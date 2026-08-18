# Shared configuration for confirmed bookings export (RP -> Polaris).
#
# Architecture: 1 master DAG triggers bookings to 3 child DAGs (routed by
# page_number % 3). Each child has max_active_runs=3. Total concurrency = 9.

region = 'us-east-1'
environment = 'pre-production'

# GraphQL configuration
graphql_endpoint = '/graphql'

# -----------------------------------------------------------------------------
# DAG scheduling / concurrency
# -----------------------------------------------------------------------------
# Master MUST be 1 — the cursor Variable is read-modify-write on every run;
# concurrent masters would race. Queueing (not parallel) is the right behavior.
max_active_runs_master = 1

# Each child can run this many instances concurrently. 3 children x 3 = 9.
max_active_runs_child = 3

# How many child DAG files we create (and route to via page_number % child_count).
child_count = 3

# Per-mutation op-DAG concurrency. Each booking-CREATE / day-UPDATE / day-DELETE
# becomes its own DAG run so a single failure can't block siblings and can be
# replayed individually from the Airflow UI. Tune up if Polaris is healthy and
# you want faster page drain; tune down if Polaris is rate-limited.
max_active_runs_op = 10

# Schedule (override per instance). None = manual trigger only.
schedule_interval = None

# -----------------------------------------------------------------------------
# Pagination
# -----------------------------------------------------------------------------
# Bookings per API page. No hard cap on pageCount — any overflow gets queued
# by child's max_active_runs.
page_size = 100

# -----------------------------------------------------------------------------
# Airflow Variable key templates (instance-prefixed in instance files)
# -----------------------------------------------------------------------------
# cursor_variable_key        = f'rp_confirmed_bookings_cursor_{instance}'
# tenant_id_variable         = f'rp_tenant_id_{instance}'
# employee_user_uri_map_variable = f'rp_employee_user_uri_map_{instance}'
# resource_planner_confirmed_bookings_export_enable_batch_task = f'resource_planner_confirmed_bookings_export_enable_batch_task_{instance}'

# -----------------------------------------------------------------------------
# Failure-notification email
# -----------------------------------------------------------------------------
# The master DAG sends one email per run when one or more page-children or
# op-DAGs failed. Override these per-instance.
#
# `email_failure_recipients`: list of addresses (To: line).
# `email_failure_subject_prefix`: prepended to the subject for filtering.
email_failure_recipients = []  # set per-instance
email_failure_subject_prefix = "[RP Confirmed Bookings Export]"
