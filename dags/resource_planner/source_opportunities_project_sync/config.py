# Shared configuration for SourceOpportunities export (RP -> Polaris project creation).
#
# Architecture: 1 master DAG fans out sourceOpportunities pages to 1 page-child
# DAG, which filters by probability and triggers 1 project-creation op-DAG per
# qualifying opportunity (one Polaris CreateProjectCopyBatch2 mutation per run).

region = 'us-east-1'
environment = 'pre-production'

# -----------------------------------------------------------------------------
# DAG scheduling / concurrency
# -----------------------------------------------------------------------------
# Master MUST be 1 — the cursor Variable is read-modify-write on every run;
# concurrent masters would race. Queueing (not parallel) is the right behavior.
max_active_runs_master = 1

# Page-child concurrency.
max_active_runs_child = 3

# Per-opportunity op-DAG concurrency. Each project creation becomes its own
# DAG run so a single failure can't block siblings and can be replayed
# individually from the Airflow UI.
max_active_runs_op = 10

# Schedule (override per instance). None = manual trigger only.
schedule_interval = None

# -----------------------------------------------------------------------------
# Pagination / filtering
# -----------------------------------------------------------------------------
# Opportunities per API page. No hard cap on pageCount — any overflow gets
# queued by the page-child's max_active_runs.
page_size = 100

# Minimum probability sent to the gateway as a server-side pre-filter (minProbability
# on both /batches and /sourceOpportunities calls).  Any opportunity below this
# will never qualify for any action, so filtering it out server-side reduces
# unnecessary data transfer.  Must be <= CLOSING_MIN_PROBABILITY.
MIN_PROBABILITY = 70

# -----------------------------------------------------------------------------
# Stage/probability routing thresholds (used by child_dag.classify_opportunities)
# -----------------------------------------------------------------------------
# Create path: stage="Closing" AND probability >= CLOSING_MIN_PROBABILITY
CLOSING_STAGE = "Closing"
CLOSING_MIN_PROBABILITY = 70

# Update-execution path: stage="Closed Won" AND probability == CLOSED_WON_PROBABILITY
CLOSED_WON_STAGE = "Closed Won"
CLOSED_WON_PROBABILITY = 100

# GraphQL enum values for putProjectWorkflowState3 mutation.
# Passed as projectWorkflowStateId — Polaris enforces valid transitions server-side.
POLARIS_INITIATE_STATE_ID  = "INITIATE"   # op-create path: new project → Initiate
POLARIS_EXECUTION_STATE_ID = "EXECUTION"  # op-update-execution path: Closed Won → Execution
POLARIS_CLOSEOUT_STATE_ID  = "CLOSEOUT"   # op-close-out path: lost/rejected → Closed

# Close-out path: one of CLOSE_OUT_STAGES AND probability == 0 → transition to "Closed"
# probability must be explicitly 0 (not null/missing) — see child_dag.classify_opportunities.
CLOSED_LOST_STAGE    = "Closed Lost"
NO_DECISION_STAGE    = "Closed/No Decision"
SALES_REJECTED_STAGE = "Sales Rejected"
CLOSE_OUT_PROBABILITY = 0
CLOSE_OUT_STAGES = frozenset({CLOSED_LOST_STAGE, NO_DECISION_STAGE, SALES_REJECTED_STAGE})

# -----------------------------------------------------------------------------
# Polaris project templates
# -----------------------------------------------------------------------------
# Confirmed 2026-08-04 (provided directly by the RP team). Values are
# Polaris project names, looked up via BulkGetProjectDetails2 the same way
# the source template is resolved.
SOW_PROJECT_TEMPLATE_NAME = "2026 - Project Template (DO NOT MODIFY)"
CHANGE_REQUEST_PROJECT_TEMPLATE_NAME = "2026 - Work Order Template (DO NOT MODIFY)"

# Maps an opportunity's engagementContractType value to the *name of the
# config attribute* (above) holding the template to use for it — not the
# template string itself, so this map and the two constants above can never
# drift out of sync with each other. Confirmed 2026-08-04 (provided directly
# by the user): "Statement of Work (SOW)" uses the SOW template; both
# "Change Order/APC" and "Work Order (WO)" use the Change Request/Work Order
# template. An unmapped value fails loudly and in isolation, see
# utils/request_payload.py:resolve_project_template_name.
ENGAGEMENT_CONTRACT_TYPE_TEMPLATE_ATTR_MAP = {
    "Statement of Work (SOW)": "SOW_PROJECT_TEMPLATE_NAME",
    "Change Order/APC": "CHANGE_REQUEST_PROJECT_TEMPLATE_NAME",
    "Work Order (WO)": "CHANGE_REQUEST_PROJECT_TEMPLATE_NAME",
}

# Static URN — copied verbatim from dags/deltek_internal/project_sync/config.py.
project_modification_save_uri = 'urn:replicon:project-modification-option:save'

# -----------------------------------------------------------------------------
# Airflow Variable key templates (instance-prefixed in instance files)
# -----------------------------------------------------------------------------
# cursor_variable_key = f'rp_source_opportunities_cursor_{instance}'
# resource_planner_source_opportunities_project_sync_enable_batch_task = f'resource_planner_source_opportunities_project_sync_enable_batch_task_{instance}'

# -----------------------------------------------------------------------------
# Failure-notification email
# -----------------------------------------------------------------------------
# The master DAG sends one email per run when one or more page-children or
# op-DAGs failed. Override these per-instance.
email_failure_recipients = []  # set per-instance
email_failure_subject_prefix = "[RP Source Opportunities Project Sync]"
