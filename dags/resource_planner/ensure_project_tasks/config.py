# Shared configuration for the ensure_project_tasks DAG.
#
# The DAG runs ASYNCHRONOUSLY from its caller — allocation-writing DAGs
# trigger it per-project and proceed immediately. The child resolves the
# project + its tasks into rp_source_time_codes so that downstream
# consumers can resolve the allocation rows that reference them.

region = 'us-east-1'
environment = 'pre-production'

# How many child runs may execute in parallel. Bursty allocation activity
# can fan out a lot of triggers in a short window; queueing past this cap
# is fine — Airflow drains it.
max_active_runs = 20

# Manual trigger only — fired by the allocation-writing DAGs.
schedule_interval = None

# -----------------------------------------------------------------------------
# Failure-notification email (override per-instance)
# -----------------------------------------------------------------------------
# Triggered DAG (fire-and-forget). When a critical task fails, an email is
# sent directly from this DAG run — no master to gather the XCom.
email_failure_recipients = []  # set per-instance
email_failure_subject_prefix = "[RP Ensure Project Tasks (JIT)]"
